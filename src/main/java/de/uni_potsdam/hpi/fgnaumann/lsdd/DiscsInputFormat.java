package de.uni_potsdam.hpi.fgnaumann.lsdd;

import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class DiscsInputFormat extends RecordInputFormat {
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset,
			int numBytes) {
		if (!super.readRecord(target, bytes, offset, numBytes)) {
			if (target.getNumFields() > 0) {
				target.setField(8, new PactString());
				return true;
			}else
				return false;
		}
		return true;
	}
}
