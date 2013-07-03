package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class ArtistNameSimilarity implements PositiveRule {
	private static ArtistNameSimilarity instance = null;
	private static AbstractStringMetric dist = new JaroWinkler();

	private ArtistNameSimilarity() {
	}

	public static ArtistNameSimilarity getInstance() {
		if (instance == null) {
			instance = new ArtistNameSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		return dist.getSimilarity(
				record1.getField(MultiBlocking.ARTIST_NAME_FIELD,
						PactString.class).getValue(),
				record2.getField(MultiBlocking.ARTIST_NAME_FIELD,
						PactString.class).getValue());
	}

	@Override
	public int getWeight() {
		return 3;
	}

	@Override
	public boolean matched(PactRecord record1, PactRecord record2) {
		// TODO Auto-generated method stub
		return false;
	}
}
