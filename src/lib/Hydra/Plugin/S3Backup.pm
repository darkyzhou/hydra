package Hydra::Plugin::S3Backup;

use strict;
use warnings;
use parent 'Hydra::Plugin';
use File::Temp;
use File::Basename;
use Fcntl;
use IO::File;
use HTTP::Tiny;
use JSON::MaybeXS qw(decode_json encode_json);
use Digest::SHA;
use Nix::Manifest;
use Nix::Config;
use Nix::Store;
use Hydra::Model::DB;
use Hydra::Helper::CatalystUtils;
use Hydra::Helper::Nix;

sub isEnabled {
    my ($self) = @_;
    return defined $self->{config}->{s3backup};
}

my $nix_store = Nix::Store->new;

my $http_client;

my $lockfile = Hydra::Model::DB::getHydraPath . "/.hydra-s3backup.lock";

sub buildFinished {
    my ($self, $build, $dependents) = @_;

    return unless $build->buildstatus == 0 or $build->buildstatus == 6;

    my $jobName = showJobName $build;
    my $job = $build->job;
    my $build_id = $build->id;

    print STDERR "S3Backup: Processing build $build_id for job '$jobName'\n";

    my $cfg = $self->{config}->{s3backup};
    my @config = defined $cfg ? ref $cfg eq "ARRAY" ? @$cfg : ($cfg) : ();

    # Find the first matching configuration (one-to-one matching)
    my $bucket_config;
    foreach my $config_item (@config) {
        if ($jobName =~ /^$config_item->{jobs}$/) {
            $bucket_config = $config_item;
            last;
        }
    }

    unless (defined $bucket_config) {
        print STDERR "S3Backup: No matching configuration for job '$jobName'\n";
        return;
    }

    unless (exists $bucket_config->{prefix} && defined $bucket_config->{prefix} && $bucket_config->{prefix} ne "") {
        die "S3Backup: prefix is required in configuration";
    }
    unless (exists $bucket_config->{publish_endpoint} && defined $bucket_config->{publish_endpoint} && $bucket_config->{publish_endpoint} ne "") {
        die "S3Backup: publish_endpoint is required in configuration";
    }
    unless (exists $bucket_config->{check_endpoint} && defined $bucket_config->{check_endpoint} && $bucket_config->{check_endpoint} ne "") {
        die "S3Backup: check_endpoint is required in configuration";
    }

    unless (defined $http_client) {
        $http_client = HTTP::Tiny->new(
            timeout => 30,
            agent => 'Hydra-S3Backup/1.0',
        );
    }

    # !!! Maybe should do per-bucket locking?
    my $lockhandle = IO::File->new;
    open($lockhandle, "+>", $lockfile) or die "Opening $lockfile: $!";
    flock($lockhandle, Fcntl::LOCK_SH) or die "Read-locking $lockfile: $!";

    # Collect initial paths from build outputs
    my @initial_paths = ();
    foreach my $output ($build->buildoutputs) {
        push @initial_paths, $output->path;
    }

    # Check if build dependencies backup is enabled
    my $backup_build_deps = 0;  # default to false
    if (exists $bucket_config->{backup_build_deps}) {
        my $value = $bucket_config->{backup_build_deps};
        $backup_build_deps = ($value eq "1" || lc($value) eq "true" || lc($value) eq "yes") ? 1 : 0;
    }
    
    # Step 1: Get derivation paths for build outputs and add them to initial_paths
    if ($backup_build_deps) {
        my %initial_paths_set = map { $_ => 1 } @initial_paths;  # Create set for deduplication
        
        foreach my $initial_path (@initial_paths) {
            my $drv_path;
            eval {
                $drv_path = $nix_store->queryDeriver($initial_path);  # 查询derivation
            };
            if ($@) {
                print STDERR "S3Backup: Warning - queryDeriver failed for $initial_path\n";
                next;
            }
            next if $drv_path eq '';
            if (!exists $initial_paths_set{$drv_path}) {
                push @initial_paths, $drv_path;
                $initial_paths_set{$drv_path} = 1;
            }
        }
    }
    
    # Step 2: Now initial_paths is ready, collect all dependencies
    my %all_paths = ();
    foreach my $initial_path (@initial_paths) {
        my @paths;
        eval {
            @paths = $nix_store->computeFSClosure(0, 1, $initial_path);
        };
        if ($@) {
            print STDERR "S3Backup: Error - computeFSClosure failed for $initial_path\n";
            next;
        }
        foreach my $path (@paths) {
            next if $path eq '';
            $all_paths{$path} = 1;
        }
    }
    
    my $tempdir = File::Temp->newdir("s3-backup-nars-$build_id" . "XXXXX", TMPDIR => 1);
    my $publish_dir = File::Temp->newdir("s3-backup-publish-$build_id" . "XXXXX", TMPDIR => 1, CLEANUP => 0);
    my $publish_path = $publish_dir->dirname;
    my $nar_dir = "$publish_path/nar";
    mkdir($nar_dir) or die "Cannot create nar directory $nar_dir: $!";
    
    my @narinfos = ();
    my $processed_paths = 0;
    my @needed_paths = keys %all_paths;
    foreach my $path (@needed_paths) {
        my $hash = substr basename($path), 0, 32;
        
        my ($deriver, $narHash, $time, $narSize, $refs) = $MACHINE_LOCAL_STORE->queryPathInfo($path, 0);
        
        my $system;
        if (defined $deriver and $MACHINE_LOCAL_STORE->isValidPath($deriver)) {
            eval {
                my $derivation = $MACHINE_LOCAL_STORE->derivationFromPath($deriver);
                $system = $derivation->{platform};
            };
            if ($@) {
                print STDERR "S3Backup: Warning - failed to get derivation info for $deriver: $@\n";
            }
        }
        
        # Check if this path already exists via HTTP endpoint
        my $prefix = $bucket_config->{prefix};
        my $key = $prefix . "$hash.narinfo";
        my $check_url = $bucket_config->{check_endpoint} . "?key=" . $key;
        
        my $should_skip = 0;
        my $response = $http_client->get($check_url);
        if ($response->{success}) {
            eval {
                my $json_data = decode_json($response->{content});
                if ($json_data->{exists}) {
                    $should_skip = 1;
                }
            };
            if ($@) {
                print STDERR "S3Backup: Warning - failed to parse check response for $key: $@\n";
            }
        } else {
            print STDERR "S3Backup: Warning - failed to check existence of $key: " . $response->{reason} . "\n";
        }
        
        next if $should_skip;

        # TODO
        
        if (system("nix --extra-experimental-features nix-command store dump-path $path > $tempdir/nar") != 0) {
            print STDERR "S3Backup: Failed to create NAR for $path\n";
            die;
        }
        
        my $digest = Digest::SHA->new(256);
        $digest->addfile("$tempdir/nar");
        my $file_hash = $digest->hexdigest;
        my @stats = stat "$tempdir/nar" or die "Couldn't stat $tempdir/nar";
        my $file_size = $stats[7];
        
        my $narinfo = "";
        $narinfo .= "StorePath: $path\n";
        $narinfo .= "URL: nar/$hash.nar\n";
        $narinfo .= "Compression: none\n";
        $narinfo .= "FileHash: sha256:$file_hash\n";
        $narinfo .= "FileSize: $file_size\n";
        $narinfo .= "NarHash: $narHash\n";
        $narinfo .= "NarSize: $narSize\n";
        $narinfo .= "References: " . join(" ", map { basename $_ } @{$refs}) . "\n";
        if (defined $deriver) {
            $narinfo .= "Deriver: " . basename $deriver . "\n";
            if (defined $system) {
                $narinfo .= "System: $system\n";
            }
        }
        
        if (exists $bucket_config->{secretKey} && defined $bucket_config->{secretKey}) {
            my $secretKey = $bucket_config->{secretKey};
            my $fingerprint = fingerprintPath($path, $narHash, $narSize, $refs);
            my $sig = signString($secretKey, $fingerprint);
            $narinfo .= "Sig: $sig\n";
        }
        
        push @narinfos, { hash => $hash, info => $narinfo };
        
        my $nar_dest = "$nar_dir/$hash.nar";
        unless (-f $nar_dest) {
            if (system("mv", "$tempdir/nar", $nar_dest) != 0) {
                print STDERR "S3Backup: Failed to move NAR file to publish directory: $nar_dest\n";
                die;
            }
        }
        
        $processed_paths++;
    }

    my $prepared_narinfos = 0;
    foreach my $info (@narinfos) {
        my $narinfo_dest = "$publish_path/" . $info->{hash} . ".narinfo";
        unless (-f $narinfo_dest) {
            if (open(my $fh, '>', $narinfo_dest)) {
                print $fh $info->{info};
                close($fh);
                $prepared_narinfos++;
            } else {
                print STDERR "S3Backup: Failed to write narinfo file: $narinfo_dest\n";
                die;
            }
        }
    }

    # Publish all files via HTTP request
    if ($prepared_narinfos > 0 || $processed_paths > 0) {
        my $prefix = $bucket_config->{prefix};
        my $publish_endpoint = $bucket_config->{publish_endpoint};
        
        my $publish_data = [
            {
                from => $publish_path,
                to => $prefix,
                delete_after_sync => JSON::MaybeXS::true
            }
        ];
        
        my $json_payload = encode_json($publish_data);
        
        print STDERR "S3Backup: Publishing to $publish_endpoint with prefix: '$prefix', folder: $publish_path\n";
        
        my $response = $http_client->post($publish_endpoint, {
            headers => {
                'Content-Type' => 'application/json',
            },
            content => $json_payload,
        });
        
        if ($response->{success}) {
            eval {
                my $response_data = decode_json($response->{content});
                if ($response_data->{success}) {
                    # pass
                } else {
                    my $error_msg = $response_data->{message} // "Unknown error";
                    print STDERR "S3Backup: Publish request failed: $error_msg\n";
                    die "S3Backup: Publish request failed: $error_msg";
                }
            };
            if ($@) {
                print STDERR "S3Backup: Failed to parse publish response: $@\n";
                print STDERR "S3Backup: Response content: " . $response->{content} . "\n";
                die "S3Backup: Failed to parse publish response";
            }
        } else {
            print STDERR "S3Backup: Failed to publish: HTTP " . $response->{status} . " - " . $response->{reason} . "\n";
            if ($response->{content}) {
                print STDERR "S3Backup: Response content: " . $response->{content} . "\n";
            }
            die "S3Backup: Failed to publish files";
        }
    } else {
        print STDERR "S3Backup: Build $build_id - no files need to be published\n";
    }
}

1;
