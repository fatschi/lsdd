package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import eu.stratosphere.pact.common.type.PactRecord;

public class AlwaysTrueSimilarity implements PositiveRule {
	private static AlwaysTrueSimilarity instance = null;

	private AlwaysTrueSimilarity() {
	}

	public static AlwaysTrueSimilarity getInstance() {
		if (instance == null) {
			instance = new AlwaysTrueSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		return 0f;
	}

	@Override
	public int getWeight() {
		return 0;
	}

	@Override
	public boolean matched(PactRecord record1, PactRecord record2) {
		return true;
	}
}
