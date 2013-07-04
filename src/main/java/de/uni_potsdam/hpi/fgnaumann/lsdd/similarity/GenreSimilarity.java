package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class GenreSimilarity implements PositiveRule {
	private static GenreSimilarity instance = null;
	private static AbstractStringMetric dist = new JaroWinkler();

	private GenreSimilarity() {
	}

	public static GenreSimilarity getInstance() {
		if (instance == null) {
			instance = new GenreSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		return dist.getSimilarity(
				record1.getField(MultiBlocking.GENRE_TITLE_FIELD,
						PactString.class).getValue(),
				record2.getField(MultiBlocking.GENRE_TITLE_FIELD,
						PactString.class).getValue());
	}

	@Override
	public int getWeight() {
		return 1;
	}
}
