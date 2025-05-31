package Hydra::Plugin;

use strict;
use warnings;
use Module::Pluggable
    search_path => "Hydra::Plugin",
    instantiate => 'new';

sub new {
    my ($class, %args) = @_;
    my $self = { db => $args{db}, config => $args{config}, plugins => $args{plugins} };
    bless $self, $class;
    return $self;
}

sub isEnabled {
    return 1;
}

sub instantiate {
    my ($class, %args) = @_;
    my $plugins = [];
    $args{plugins} = $plugins;
    
    print STDERR "Plugin: Starting plugin instantiation\n";
    
    my @all_plugins = $class->plugins(%args);
    print STDERR "Plugin: Found " . scalar(@all_plugins) . " total plugins\n";
    
    my @enabled_plugins = grep { $_->isEnabled } @all_plugins;
    print STDERR "Plugin: " . scalar(@enabled_plugins) . " plugins are enabled\n";
    
    foreach my $plugin (@enabled_plugins) {
        my $plugin_name = ref($plugin);
        print STDERR "Plugin: Loaded plugin: $plugin_name\n";
    }
    
    push @$plugins, @enabled_plugins;
    
    print STDERR "Plugin: Plugin instantiation completed with " . scalar(@$plugins) . " active plugins\n";
    
    return @$plugins;
}

# To implement behaviors in response to the following events, implement
# the function in your plugin and it will be executed by hydra-notify.
#
# See the tests in t/Event/*.t for arguments, and the documentation for
# notify events for semantics.
#

# # Called when an evaluation of $jobset has begun.
# sub evalStarted {
#     my ($self, $traceID, $jobset) = @_;
# }

# # Called when an evaluation of $jobset determined the inputs had not changed.
# sub evalCached {
#     my ($self, $traceID, $jobset, $evaluation) = @_;
# }

# # Called when an evaluation of $jobset failed.
# sub evalFailed {
#     my ($self, $traceID, $jobset) = @_;
# }

# # Called when $evaluation of $jobset has completed successfully.
# sub evalAdded {
#     my ($self, $traceID, $jobset, $evaluation) = @_;
# }

# # Called when build $build has been queued.
# sub buildQueued {
#     my ($self, $build) = @_;
# }

# # Called when build $build has been queued again by evaluation $evaluation
# where $build has not yet finished.
# sub cachedBuildQueued {
#     my ($self, $evaluation, $build) = @_;
# }

# # Called when build $build is a finished build, and is
# part evaluation $evaluation
# sub cachedBuildFinished {
#     my ($self, $evaluation, $build) = @_;
# }

# # Called when build $build has started.
# sub buildStarted {
#     my ($self, $build) = @_;
# }

# # Called when build $build has finished.  If the build failed, then
# # $dependents is an array ref to a list of builds that have also
# # failed as a result (i.e. because they depend on $build or a failed
# # dependeny of $build).
# sub buildFinished {
#     my ($self, $build, $dependents) = @_;
# }

# # Called when step $step has finished. The build log is stored in the
# # file $logPath (bzip2-compressed).
# sub stepFinished {
#     my ($self, $step, $logPath) = @_;
# }

# Called to determine the set of supported input types.  The plugin
# should add these to the $inputTypes hashref, e.g. $inputTypes{'svn'}
# = 'Subversion checkout'.
sub supportedInputTypes {
    my ($self, $inputTypes) = @_;
}

# Called to fetch an input of type ‘$type’.  ‘$value’ is the input
# location, typically the repository URL.
sub fetchInput {
    my ($self, $type, $name, $value, $project, $jobset) = @_;
    return undef;
}

# Get the commits to repository ‘$value’ between revisions ‘$rev1’ and
# ‘$rev2’.  Each commit should be a hash ‘{ revision = "..."; author =
# "..."; email = "..."; }’.
sub getCommits {
    my ($self, $type, $value, $rev1, $rev2) = @_;
    return [];
}

1;
