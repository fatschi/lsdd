package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import eu.stratosphere.pact.common.type.PactRecord;

public interface NegativeRule {
	public boolean duplicateRuledOut(PactRecord record1, PactRecord record2);
}
