package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import eu.stratosphere.pact.common.type.PactRecord;

public class TracksSimilarity implements PositiveRule{
	private static TracksSimilarity instance = null;

	private TracksSimilarity() {
	}

	public static TracksSimilarity getInstance() {
		if (instance == null) {
			instance = new TracksSimilarity();
		}
		return instance;
	}

	@Override
	public int getWeight() {
		return 5;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		return 0;
	}

}
