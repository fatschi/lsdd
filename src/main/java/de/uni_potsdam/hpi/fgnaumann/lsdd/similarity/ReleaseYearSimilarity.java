package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class ReleaseYearSimilarity implements PositiveRule {
	private static ReleaseYearSimilarity instance = null;

	private ReleaseYearSimilarity() {
	}

	public static ReleaseYearSimilarity getInstance() {
		if (instance == null) {
			instance = new ReleaseYearSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		String record1DiscRelease = record1
				.getField(MultiBlocking.DISC_RELEASED_FIELD, PactString.class)
				.getValue().toLowerCase();
		String record2DiscRelease = record2
				.getField(MultiBlocking.DISC_RELEASED_FIELD, PactString.class)
				.getValue().toLowerCase();
		if (!record1DiscRelease.isEmpty() && !record2DiscRelease.isEmpty()) {
			try {
				return 1f / (Math.abs(Integer.valueOf(record1DiscRelease)
						- Integer.valueOf(record2DiscRelease)) + 1);
			} catch (NumberFormatException nfe) {
			}
		}
		return MultiBlocking.SIMILARITY_THRESHOLD;
	}

	@Override
	public int getWeight() {
		return 3;
	}
}
