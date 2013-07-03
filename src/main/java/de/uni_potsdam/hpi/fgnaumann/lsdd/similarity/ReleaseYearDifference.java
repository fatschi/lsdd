package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class ReleaseYearDifference implements NegativeRule {
	private static ReleaseYearDifference instance = null;
	private static int MAX_YEAR_DIFF = 0;

	private ReleaseYearDifference() {
	}

	public static ReleaseYearDifference getInstance() {
		if (instance == null) {
			instance = new ReleaseYearDifference();
		}
		return instance;
	}

	@Override
	public boolean duplicateRuledOut(PactRecord record1, PactRecord record2) {
		String record1DiscRelease = record1
				.getField(MultiBlocking.DISC_RELEASED_FIELD, PactString.class)
				.getValue().toLowerCase();
		String record2DiscRelease = record2
				.getField(MultiBlocking.DISC_RELEASED_FIELD, PactString.class)
				.getValue().toLowerCase();
		if (!record1DiscRelease.isEmpty() && !record2DiscRelease.isEmpty()) {
			try {
				if (Math.abs(Integer.valueOf(record1DiscRelease)
						- Integer.valueOf(record2DiscRelease)) > MAX_YEAR_DIFF) {
					return true;
				}
			} catch (NumberFormatException nfe) {
				return false;
			}
		}
		return false;
	}
}
