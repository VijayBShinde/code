package org.dm.mr;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 * 
 * @author Vijay.Shinde
 *
 */
@SuppressWarnings("unchecked")
public class MyGenericWritable extends GenericWritable{

	private static Class<? extends Writable>[] CLASSES = null;

	static {
		CLASSES = (Class<? extends Writable>[]) new Class[] {
			UsersRecord.class,
			CommentsRecord.class
		};
	}

	public MyGenericWritable() {}

	public MyGenericWritable(Writable instance) {
		set(instance);
	}

	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}


}
