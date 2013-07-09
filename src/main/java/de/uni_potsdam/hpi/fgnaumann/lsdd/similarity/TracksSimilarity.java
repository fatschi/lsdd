package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import de.uni_potsdam.hpi.fgnaumann.lsdd.TrackList;
import de.uni_potsdam.hpi.fgnaumann.lsdd.util.UnicodeUtils;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class TracksSimilarity implements PositiveRule {
	private static final int WINDOW_SIZE = 5;

	private static TracksSimilarity instance = null;
	private static AbstractStringMetric distance = new JaccardSimilarity();

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
		if (trackList1.size() == 0)
			return 0f;

		TrackList trackList2 = record2.getField(MultiBlocking.TRACKS_FIELD,
				TrackList.class);
		if (trackList2.size() == 0)
			return 0f;

		if (trackList1.size() < trackList2.size()) {
			TrackList tempSwap = trackList1;
			trackList1 = trackList2;
			trackList2 = tempSwap;
		}

		int i = 0;
		for (; i < trackList1.size(); i++) {
			String trackNameOuter = UnicodeUtils
					.removeNonAlphaNumeric(trackList1.get(i).getField(
							MultiBlocking.TRACK_TITLE_FIELD, PactString.class));
			int trackNumberOuter = trackList1
					.get(i)
					.getField(MultiBlocking.TRACK_NUMBER_FIELD,
							PactInteger.class).getValue();
			float highest = 0f;
			inner_for: for (int j = 0, k = i - WINDOW_SIZE / 2; j <= WINDOW_SIZE; k++, j++) {
				if (k < 0 || k >= trackList2.size())
					continue inner_for;
				String trackNameInner = UnicodeUtils
						.removeNonAlphaNumeric(trackList2.get(k).getField(
								MultiBlocking.TRACK_TITLE_FIELD,
								PactString.class));
				if (!trackNameInner.isEmpty() && !trackNameOuter.isEmpty()) {
					int trackNumberInner = trackList2
							.get(k)
							.getField(MultiBlocking.TRACK_NUMBER_FIELD,
									PactInteger.class).getValue();

					int trackDifference = Math.abs(trackNumberOuter
							- trackNumberInner);
					final int maxDifference = Math.min(trackNumberOuter,
							trackNumberInner);
					float differencePunishmentFactor = 0;
					if (trackDifference <= maxDifference) {
						differencePunishmentFactor = 1 - trackDifference
								/ maxDifference;
					}

					float currentSimilarity = distance.getSimilarity(
							trackNameOuter, trackNameInner)
							* differencePunishmentFactor;
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
		return 7;
	}

}
