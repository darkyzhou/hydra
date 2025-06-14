package Hydra::Plugin::S3Backup;

use strict;
use warnings;
use parent 'Hydra::Plugin';
use File::Temp;
use File::Basename;
use Fcntl;
use IO::File;
use Net::Amazon::S3;
use Net::Amazon::S3::Client;
use Net::Amazon::S3::Vendor::Generic;
use Net::Amazon::S3::Authorization::Basic;
use Digest::SHA;
use Nix::Config;
use Nix::Store;
use Hydra::Model::DB;
use Hydra::Helper::CatalystUtils;
use Hydra::Helper::Nix;

sub isEnabled {
    my ($self) = @_;
    return defined $self->{config}->{s3backup};
}

my $client;
my %compressors = ();

$compressors{"none"} = "";

if (defined($Nix::Config::bzip2)) {
    $compressors{"bzip2"} = "| $Nix::Config::bzip2",
}

if (defined($Nix::Config::xz)) {
    $compressors{"xz"} = "| $Nix::Config::xz",
}

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

    my @matching_configs = ();
    foreach my $bucket_config (@config) {
        push @matching_configs, $bucket_config if $jobName =~ /^$bucket_config->{jobs}$/;
    }

    unless (@matching_configs) {
        print STDERR "S3Backup: No matching configurations for job '$jobName'\n";
        return;
    }

    unless (defined $client) {
        my $vendor;
        my $authorization_context;
        
        # Check for custom S3 vendor configuration in matching_configs
        foreach my $bucket_config (@matching_configs) {
            # Check for required authorization credentials
            if (exists $bucket_config->{access_key_id} && exists $bucket_config->{access_key_secret}) {
                $authorization_context = Net::Amazon::S3::Authorization::Basic->new(
                    aws_access_key_id     => $bucket_config->{access_key_id},
                    aws_secret_access_key => $bucket_config->{access_key_secret},
                );
            }
            
            if (exists $bucket_config->{host}) {
                my %vendor_params = ( 
                    host => $bucket_config->{host},
                );
                
                # Handle use_https - convert string to boolean
                if (exists $bucket_config->{use_https}) {
                    my $use_https = $bucket_config->{use_https};
                    $vendor_params{use_https} = ($use_https eq "1" || lc($use_https) eq "true") ? 1 : 0;
                }
                
                # Handle use_virtual_host - convert string to boolean  
                if (exists $bucket_config->{use_virtual_host}) {
                    my $use_virtual_host = $bucket_config->{use_virtual_host};
                    $vendor_params{use_virtual_host} = ($use_virtual_host eq "1" || lc($use_virtual_host) eq "true") ? 1 : 0;
                }
                
                $vendor = Net::Amazon::S3::Vendor::Generic->new(%vendor_params);
            }
        }
        
        # Check if required credentials are provided
        unless (defined $authorization_context) {
            die "S3Backup: access_key_id and access_key_secret are required in bucket configuration";
        }
        
        eval {
            my %s3_params = (
                authorization_context => $authorization_context,
                retry => 1,
            );
            
            if (defined $vendor) {
                $s3_params{vendor} = $vendor;
            }
            
            $client = Net::Amazon::S3::Client->new( 
                s3 => Net::Amazon::S3->new(%s3_params)
            );
        };
        if ($@) {
            return;
        }
    }

    # !!! Maybe should do per-bucket locking?
    my $lockhandle = IO::File->new;
    open($lockhandle, "+>", $lockfile) or die "Opening $lockfile: $!";
    flock($lockhandle, Fcntl::LOCK_SH) or die "Read-locking $lockfile: $!";

    my @needed_paths = ();
    foreach my $output ($build->buildoutputs) {
        push @needed_paths, $output->path;
    }

    # Check if drvpath backup is enabled (default: true for backward compatibility)  
    my $backup_drvpath = 1;  # default to enabled
    foreach my $bucket_config (@matching_configs) {
        if (exists $bucket_config->{backup_drvpath}) {
            my $value = $bucket_config->{backup_drvpath};
            $backup_drvpath = ($value eq "1" || lc($value) eq "true" || lc($value) eq "yes") ? 1 : 0;
            last;  # Use the first config's setting
        }
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
            print STDERR "S3Backup: Adding drvpath: $drvpath\n";
        } else {
            print STDERR "S3Backup: Warning - drvpath is undefined or empty\n";
        }
    } else {
        print STDERR "S3Backup: drvpath backup is disabled\n";
    }

    my %narinfos = ();
    my %compression_types = ();
    foreach my $bucket_config (@matching_configs) {
        my $compression_type =
          exists $bucket_config->{compression_type} ? $bucket_config->{compression_type} : "bzip2";
        die "Unsupported compression type $compression_type" unless exists $compressors{$compression_type};
        if (exists $compression_types{$compression_type}) {
            push @{$compression_types{$compression_type}}, $bucket_config;
        } else {
            $compression_types{$compression_type} = [ $bucket_config ];
            $narinfos{$compression_type} = [];
        }
    }

    my $tempdir = File::Temp->newdir("s3-backup-nars-$build_id" . "XXXXX", TMPDIR => 1);

    my %seen = ();
    my $processed_paths = 0;
    # Upload nars and build narinfos
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
        
        foreach my $compression_type (keys %compression_types) {
            my $configs = $compression_types{$compression_type};
            my @incomplete_buckets = ();
            # Don't do any work if all the buckets have this path
            foreach my $bucket_config (@{$configs}) {
                my $bucket = $client->bucket( name => $bucket_config->{name} );
                my $prefix = exists $bucket_config->{prefix} ? $bucket_config->{prefix} : "";
                unless ($bucket->object( key => $prefix . "$hash.narinfo" )->exists) {
                    push @incomplete_buckets, $bucket_config;
                }
            }
            next unless @incomplete_buckets;
            
            my $compressor = $compressors{$compression_type};
            if (system("nix-store --dump $path $compressor > $tempdir/nar") != 0) {
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
            $narinfo .= "Compression: $compression_type\n";
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
            push @{$narinfos{$compression_type}}, { hash => $hash, info => $narinfo };
            
            foreach my $bucket_config (@incomplete_buckets) {
                my $bucket = $client->bucket( name => $bucket_config->{name} );
                my $prefix = exists $bucket_config->{prefix} ? $bucket_config->{prefix} : "";
                my $nar_object = $bucket->object(
                    key => $prefix . "$hash.nar",
                    content_type => "application/x-nix-archive"
                );
                print STDERR "S3Backup: Uploading NAR to bucket '" . $bucket_config->{name} . "': $prefix$hash.nar\n";
                eval {
                    $nar_object->put_filename("$tempdir/nar");
                };
                if ($@) {
                    print STDERR "S3Backup: Failed to upload NAR to bucket '" . $bucket_config->{name} . "': $@\n";
                    die;
                }
            }
        }
        $processed_paths++;
    }

    # Upload narinfos
    my $uploaded_narinfos = 0;
    foreach my $compression_type (keys %narinfos) {
        my $infos = $narinfos{$compression_type};
        foreach my $bucket_config (@{$compression_types{$compression_type}}) {
            foreach my $info (@{$infos}) {
                my $bucket = $client->bucket( name => $bucket_config->{name} );
                my $prefix = exists $bucket_config->{prefix} ? $bucket_config->{prefix} : "";
                my $narinfo_object = $bucket->object(
                    key => $prefix . $info->{hash} . ".narinfo",
                    content_type => "text/x-nix-narinfo"
                );
                unless ($narinfo_object->exists) {
                    print STDERR "S3Backup: Uploading narinfo to bucket '" . $bucket_config->{name} . "': $prefix" . $info->{hash} . ".narinfo\n";
                    eval {
                        $narinfo_object->put($info->{info});
                    };
                    if ($@) {
                        print STDERR "S3Backup: Failed to upload narinfo to bucket '" . $bucket_config->{name} . "': $@\n";
                        die;
                    }
                    $uploaded_narinfos++;
                }
            }
        }
    }

    print STDERR "S3Backup: Build $build_id backup completed successfully. Uploaded $uploaded_narinfos narinfos\n";
}

1;
