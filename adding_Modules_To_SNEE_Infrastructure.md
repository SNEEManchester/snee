# Introduction #

This is a hopefully best standard behaviour for dealing with adding new modules to the SNEE infrastructure.


# Adding module #
(tips to keep sure you don't add or miss the wrong stuff into the svn)

  1. Its safer to create a new module from scratch with new-other-maven-module (porting a copy of a current module will cause many problems with maven) but its easier and safer to copy the pom file and altering its artifiact id, etc.
  1. You need to update the main SNEE pom with the new module name (the artifact id of the module from point 1).
  1. you need to add the new modules directories to the svn (**but putting any of the auto generated files like target directory, .settings, .protect to do this, right click on each folder, and go to team->svn ignore**) To generate all these folders, it is recommended you do a mvn clean,install,package as this will generate all the folders.
  1. right click on your module and click share. This allows it to be downloaded off the repos.
  1. commit new module to svn.
  1. send a mail out to the rest of the developing team announcing the addition of the module.

# gaining modules from merges #
(tips to keep sure you actually get the modules)

  1. merge the revisions which give you the new module.
  1. commit your new merged branch to the svn.
  1. remove your copy of the branch from your local area (needed for eclipse to detect the new module).
  1. pull your branch off the svn repos, if all is good and worked, you should find the new module appears in your new local version.