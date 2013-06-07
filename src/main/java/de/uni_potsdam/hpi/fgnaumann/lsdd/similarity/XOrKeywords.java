package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class XOrKeywords implements NegativeRule{
    private static XOrKeywords instance = null;
    private static String[] keywords = {"remix", "remaster"}; 
 
    private XOrKeywords() {}
 
    public static XOrKeywords getInstance() {
        if (instance == null) {
            instance = new XOrKeywords();
        }
        return instance;
    }

	@Override
	public boolean duplicateRuledOut(PactRecord record1, PactRecord record2) {
		String record1DiscTitle = record1.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class).getValue().toLowerCase();
		String record2DiscTitle = record2.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class).getValue().toLowerCase();
		for(String keyword:keywords){
			if((record1DiscTitle.contains(keyword)&&!record2DiscTitle.contains(keyword))
					|| (!record1DiscTitle.contains(keyword)&&record2DiscTitle.contains(keyword))) 
				return true;
		}
		return false;
	}
}