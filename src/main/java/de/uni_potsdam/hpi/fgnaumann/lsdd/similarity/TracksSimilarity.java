package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import de.uni_potsdam.hpi.fgnaumann.lsdd.TrackList;
import de.uni_potsdam.hpi.fgnaumann.lsdd.util.UnicodeUtils;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class TracksSimilarity implements PositiveRule {
	private static final int WINDOW_SIZE = 3;

	private static TracksSimilarity instance = null;
	private static AbstractStringMetric dist1 = new Levenshtein();
	private static AbstractStringMetric dist2 = new JaroWinkler();
	private static AbstractStringMetric dist3 = new JaccardSimilarity();

	private TracksSimilarity() {
	}

	public static TracksSimilarity getInstance() {
		if (instance == null) {
			instance = new TracksSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		float similarity = 0f;

		TrackList trackList1 = record1.getField(MultiBlocking.TRACKS_FIELD,
				TrackList.class);

		TrackList trackList2 = record2.getField(MultiBlocking.TRACKS_FIELD,
				TrackList.class);

		if (trackList1.size() < trackList2.size()) {
			TrackList tempSwap = trackList1;
			trackList1 = trackList2;
			trackList2 = tempSwap;
		}

		int i = 0;
		for (; i < trackList1.size(); i++) {
			String trackNameOuter = UnicodeUtils.removeNonAlphaNumeric(trackList1.get(i).getField(
					MultiBlocking.TRACK_TITLE_FIELD, PactString.class));
			int trackNumberOuter = trackList1.get(i).getField(
					MultiBlocking.TRACK_NUMBER_FIELD, PactInteger.class)
					.getValue();
			float highest = 0f;
			inner_for: for (int j = 0, k = i - WINDOW_SIZE / 2; j <= WINDOW_SIZE; k++, j++) {
				if (k < 0 || k >= trackList2.size())
					continue inner_for;
				String trackNameInner = UnicodeUtils.removeNonAlphaNumeric(trackList2.get(k).getField(
						MultiBlocking.TRACK_TITLE_FIELD, PactString.class));
				if(!trackNameInner.isEmpty() && !trackNameOuter.isEmpty())	{
					int trackNumberInner = trackList2.get(k).getField(
							MultiBlocking.TRACK_NUMBER_FIELD, PactInteger.class)
							.getValue();
					
					int trackDifference = (Math.abs(trackNumberOuter - trackNumberInner));
					float differencePunishmentFactor = 0;
					if (trackDifference == 0) {
						differencePunishmentFactor = 1;
					} else if (trackDifference == 1) {
						differencePunishmentFactor = MultiBlocking.SIMILARITY_THRESHOLD + 0.1f;
					} else if (trackDifference == 2) {
						differencePunishmentFactor = MultiBlocking.SIMILARITY_THRESHOLD;
					} else if (trackDifference == 3) {
						differencePunishmentFactor = MultiBlocking.SIMILARITY_THRESHOLD - 0.1f;
					} else {
						differencePunishmentFactor = 0;
					}
					
					float currentSimilarity = ((dist1.getSimilarity(trackNameOuter,
							trackNameInner)
							+ dist2.getSimilarity(trackNameOuter, trackNameInner) + dist3
								.getSimilarity(trackNameOuter, trackNameInner)) / 3)*differencePunishmentFactor;
					if (currentSimilarity > highest)
						highest = currentSimilarity;
				}
			}
			similarity += highest;
		}
		if (i == 0)
			return MultiBlocking.SIMILARITY_THRESHOLD;
		else
			return similarity / i;
	}
	
	@Override
	public int getWeight() {
		return 5;
	}

}
