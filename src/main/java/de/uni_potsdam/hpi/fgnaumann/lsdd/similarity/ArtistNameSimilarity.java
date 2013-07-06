package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class ArtistNameSimilarity implements PositiveRule {
	private static ArtistNameSimilarity instance = null;
	private static AbstractStringMetric dist1 = new Levenshtein();
	private static AbstractStringMetric dist2 = new JaroWinkler();
	private static AbstractStringMetric dist3 = new JaccardSimilarity();

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
		String artistName1 = record1.getField(MultiBlocking.ARTIST_NAME_FIELD,
				PactString.class).getValue();
		String artistName2 = record2.getField(MultiBlocking.ARTIST_NAME_FIELD,
				PactString.class).getValue();

		return (dist1.getSimilarity(artistName1, artistName2)
				+ dist2.getSimilarity(artistName1, artistName2) + dist3
					.getSimilarity(artistName1, artistName2)) / 3;
	}

	@Override
	public int getWeight() {
		return 5;
	}
}
