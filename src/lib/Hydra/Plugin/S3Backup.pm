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

    my @needed_paths = ();
    foreach my $output ($build->buildoutputs) {
        push @needed_paths, $output->path;
    }

    # Check if drvpath backup is enabled
    my $backup_drvpath = 0;  # default to false
    if (exists $bucket_config->{backup_drvpath}) {
        my $value = $bucket_config->{backup_drvpath};
        $backup_drvpath = ($value eq "1" || lc($value) eq "true" || lc($value) eq "yes") ? 1 : 0;
    }

    # Safely add drvpath with defensive checks (if enabled)
    if ($backup_drvpath) {
        my $drvpath;
        
        # Try different methods to get drvpath
        eval {
            # Method 1: Try accessing via DBIx::Class method
            if ($build->can('drvpath')) {
                $drvpath = $build->drvpath;
            }
            # Method 2: Try get_column if available
            elsif ($build->can('get_column')) {
                $drvpath = $build->get_column('drvpath');
            }
            # Method 3: Try accessing _column_data directly
            elsif (ref($build) eq 'HASH' && exists $build->{_column_data} 
                   && ref($build->{_column_data}) eq 'HASH' 
                   && exists $build->{_column_data}->{drvpath}) {
                $drvpath = $build->{_column_data}->{drvpath};
            }
        };
        
        if ($@) {
            print STDERR "S3Backup: Warning - could not access drvpath: $@\n";
        } elsif (defined $drvpath && $drvpath ne '') {
            push @needed_paths, $drvpath;
        } else {
            print STDERR "S3Backup: Warning - drvpath is undefined or empty\n";
        }
    } else {
        print STDERR "S3Backup: drvpath backup is disabled\n";
    }

    my $tempdir = File::Temp->newdir("s3-backup-nars-$build_id" . "XXXXX", TMPDIR => 1);

    my %seen = ();
    my $processed_paths = 0;
    # Upload nars and build narinfos
    my $publish_dir = File::Temp->newdir("s3-backup-publish-$build_id" . "XXXXX", TMPDIR => 1, CLEANUP => 0);
    my $publish_path = $publish_dir->dirname;
    
    my @narinfos = ();
    
    while (@needed_paths) {
        my $path = shift @needed_paths;
        next if exists $seen{$path};
        $seen{$path} = undef;
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
        
        foreach my $reference (@{$refs}) {
            push @needed_paths, $reference;
        }
        
        # Check if this path already exists via HTTP endpoint
        my $prefix = $bucket_config->{prefix};
        my $key = $prefix . "$hash.narinfo";
        my $check_url = $bucket_config->{check_endpoint} . "?key=" . $key;
        
        my $response = $http_client->get($check_url);
        if ($response->{success}) {
            eval {
                my $json_data = decode_json($response->{content});
                if ($json_data->{exists}) {
                    print STDERR "S3Backup: File $key already exists, skipping\n";
                    next; # Skip if already exists
                }
            };
            if ($@) {
                print STDERR "S3Backup: Warning - failed to parse check response for $key: $@\n";
                # Continue processing if we can't parse the response
            }
        } else {
            print STDERR "S3Backup: Warning - failed to check existence of $key: " . $response->{reason} . "\n";
            # Continue processing if check fails
        }
        
        # Create NAR without compression (nix-store --dump no longer supports compressor)
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
        $narinfo .= "URL: $hash.nar\n";
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
        
        my $nar_dest = "$publish_path/$hash.nar";
        unless (-f $nar_dest) {
            if (system("mv", "$tempdir/nar", $nar_dest) != 0) {
                print STDERR "S3Backup: Failed to move NAR file to publish directory: $nar_dest\n";
                die;
            }
            print STDERR "S3Backup: Prepared NAR for publishing: $hash.nar\n";
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
                print STDERR "S3Backup: Prepared narinfo for publishing: " . $info->{hash} . ".narinfo\n";
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
        
        my $publish_data = {
            folder_path => $publish_path,
            prefix => $prefix
        };
        
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
                    print STDERR "S3Backup: Build $build_id backup completed successfully. " . 
                                ($response_data->{message} // "Published successfully") . "\n";
                    print STDERR "S3Backup: Prepared $prepared_narinfos narinfos for publishing\n";
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
