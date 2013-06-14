package de.uni_potsdam.hpi.fgnaumann.lsdd.util;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * This class takes care of writing duplicate to the output. It stores the
 * disc-record with the lower id in the first field of the output tuple.
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */

public class DuplciateEmitter {
	private Collector<PactRecord> out;

	public DuplciateEmitter(Collector<PactRecord> out) {
		this.out = out;
	}

	public void emitDuplicate(PactRecord record1, PactRecord record2) {
		PactRecord outputRecord = new PactRecord();
		if (record1.getField(0, PactInteger.class).getValue() < record2
				.getField(0, PactInteger.class).getValue()) {
			outputRecord.setField(MultiBlocking.DUPLICATE_ID_1_FIELD,
					record1.getField(0, PactInteger.class));
			outputRecord.setField(MultiBlocking.DUPLICATE_ID_2_FIELD,
					record2.getField(0, PactInteger.class));
		} else {
			outputRecord.setField(MultiBlocking.DUPLICATE_ID_2_FIELD,
					record1.getField(0, PactInteger.class));
			outputRecord.setField(MultiBlocking.DUPLICATE_ID_1_FIELD,
					record2.getField(0, PactInteger.class));
		}
		out.collect(outputRecord);
	}

}