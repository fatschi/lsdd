package de.uni_potsdam.hpi.fgnaumann.lsdd;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public abstract class BlockingFunction {
	
	abstract PactString function(PactRecord record);
	
	public PactRecord copyWithBlockingKey(PactRecord record){
		PactRecord nr = record.createCopy();
		nr.setField(MultiBlocking.BLOCKING_KEY_FIELD, function(record));
		return nr;
	}
}
