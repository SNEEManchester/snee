### Small is nice ###

Keep methods small. Keep classes small. Wrap lines at 70 characters.

### Testing ###

Every public method should have a test associated with it, and tests should cover all the functionality of the method.

### Logger ###

We are using the Apache Logger package.  You should include it by importing the respective classs:

```
import org.apache.log4j.Logger;
```

and then creating an instance of the logger as a field variable of the class:

```
	/**
	 * Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(QueryCompiler.class.getName());
```

Our convention is to add logger statement at the start and end of each method.

For the start of a private method:

```
	private void processLogicalSchema(String logicalSchemaFile) 
	throws SchemaMetadataException, MetadataException, 
	TypeMappingException, UnsupportedAttributeTypeException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER processLogicalSchema() with " +
					logicalSchemaFile);
		}
```

Note that all the method parameters are displayed.

For the end of a private method:

```
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN processLogicalSchema() #extents=" + 
					_schema.size());
		}
	}
```

For the start of a public method:

```
	public void addExtent(ExtentMetadata extent) 
	throws MetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER addExtent() with " + extent);

```

For the end of a public method:

```
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN addExtent()");
		}
	}
```


### Naming ###

All names of classes, variables, etc., should be in English and should be meaningful.  Please see the following naming conventions suggested by http://java.sun.com/docs/codeconv/html/CodeConventions.doc8.html#367.

### Code Management ###

The proposal is for each developer to work on his/her own feature [branch](http://svnbook.red-bean.com/nightly/en/svn.branchmerge.whatis.html), which then may be reintegrated into the trunk if deemed appropriate.

Briefly, the mode of operation is as follows:

  * Firstly, create a branch as described in [Creating a Branch](http://svnbook.red-bean.com/nightly/en/svn.branchmerge.using.html#svn.branchmerge.using.create).  Our branch naming convention is to have the developer name and date in the format `googleUsername_DDMMYY`.  A comment describing the purpose of the branch is helpful.
  * Check out or switch to the branch that you have created.  Work on this branch, and commit changes to it as appropriate.  See [working with your branch](http://svnbook.red-bean.com/nightly/en/svn.branchmerge.using.html#svn.branchmerge.using.work).
  * It is important to keep the feature branch synchronised with the trunk.  Periodically, merge in changes from trunk to feature branch (once or twice a week if poss).  This prevents the feature branch and the trunk from drifting too far apart.   See [Keeping a branch in Sync](http://svnbook.red-bean.com/nightly/en/svn.branchmerge.basicmerging.html#svn.branchemerge.basicmerging.stayinsync).
  * When the feature is complete, we may agree to reintegrate the branch back into the trunk.  The procedure is described in [Reintegrating a Branch](http://svnbook.red-bean.com/nightly/en/svn.branchmerge.basicmerging.html#svn.branchemerge.basicmerging.reintegrate).

Note: The links above should be used mainly to understand the concepts involved. We recommend using the Subclipse plugin to carry out branching and merging operations, as it provides an easy-to-use interface.

### Separate Interface from Implementation ###

When dealing with Collections, Lists, Maps, Sets it is best to declare variables as

`List<String> name = new ArrayList<String>();`

i.e., you use the interface on the left hand side and your choice of implementation on the right hand side.

### Other ###

Always use curly braces after an if statement condition, even if it only one line long, i.e.,
	if (<<condition>>) {
		<<action>>
	}```