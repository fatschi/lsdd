package de.uni_potsdam.hpi.fgnaumann.lsdd;

import java.util.Comparator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class BlockingKeyComparator implements Comparator<PactRecord> {

	@Override
	public int compare(PactRecord r1, PactRecord r2) {
		return r1.getField(MultiBlocking.BLOCKING_ID_FIELD, PactString.class)
				.compareTo(
						r1.getField(MultiBlocking.BLOCKING_ID_FIELD,
								PactString.class));
	}

}
