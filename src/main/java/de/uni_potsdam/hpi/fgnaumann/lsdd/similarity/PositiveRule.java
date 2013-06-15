package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import eu.stratosphere.pact.common.type.PactRecord;

public interface PositiveRule {
	public int getWeight();
	public float similarity(PactRecord record1, PactRecord record2);
	public boolean matched(PactRecord record1, PactRecord record2);
}
