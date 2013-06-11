package de.uni_potsdam.hpi.fgnaumann.lsdd;

import java.util.HashSet;
import java.util.Set;

import de.uni_potsdam.hpi.fgnaumann.lsdd.util.AsciiUtils;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public abstract class BlockingFunction {
	
	static final @SuppressWarnings({ "serial" })
	public
	Set<BlockingFunction> blockingFuntions = new HashSet<BlockingFunction>() {
		{
			add(new BlockingFunction() {
				@Override
				public
				PactString getID(){
					return new PactString("Genre2Year3");
				}
				
				@Override
				PactString explode(PactRecord record){
					String genre = record.getField(4, PactString.class)
							.getValue().replace("\"", "").replaceAll("[^a-zA-Z0-9]","");
					String year = record.getField(5, PactString.class)
							.getValue().replace("\"", "").replaceAll("[^a-zA-Z0-9]","");
					PactString blockingKey = new PactString(genre + year);
					AsciiUtils.toLowerCase(blockingKey);
					return blockingKey;						
				}
				
				@Override
				PactString function(PactRecord record) {
					String genre = record.getField(4, PactString.class)
							.getValue().replace("\"", "").replaceAll("[^a-zA-Z0-9]","");
					genre = genre.length() > 2 ? genre.substring(0, 2) : "";
					String year = record.getField(5, PactString.class)
							.getValue().replace("\"", "").replaceAll("[^a-zA-Z0-9]","");
					year = year.length() >= 4 ? year.substring(0, 3) : "";
					PactString blockingKey = new PactString(genre + year);
					AsciiUtils.toLowerCase(blockingKey);
					return blockingKey;
				}
			}
			);
			add(new BlockingFunction() {
				@Override
				public
				PactString getID(){
					return new PactString("Artist2Year3");
				}
				
				@Override
				PactString explode(PactRecord record){
					String artist = record.getField(2, PactString.class)
							.getValue().replace("\"", "").replaceAll("[^a-zA-Z0-9]","");
					String year = record.getField(5, PactString.class)
							.getValue().replace("\"", "").replaceAll("[^a-zA-Z0-9]","");
					PactString blockingKey = new PactString(artist + year);
					AsciiUtils.toLowerCase(blockingKey);
					return blockingKey;						
				}
				@Override
				PactString function(PactRecord record) {
					String artist = record.getField(2, PactString.class)
							.getValue().replace("\"", "").replaceAll("[^a-zA-Z0-9]","");
					artist = artist.length() > 2 ? artist.substring(0, 2)
							: "";
					String year = record.getField(5, PactString.class)
							.getValue().replace("\"", "").replaceAll("[^a-zA-Z0-9]","");
					year = year.length() >= 4 ? year.substring(0, 3) : "";
					PactString blockingKey = new PactString(artist + year);
					AsciiUtils.toLowerCase(blockingKey);
					return blockingKey;
				}
			}
			);
		}
	};
	
	public abstract PactString getID();
	
	abstract PactString explode(PactRecord record);
	
	abstract PactString function(PactRecord record);
	
	public PactRecord copyWithBlockingKey(PactRecord record){
		PactRecord nr = record.createCopy();
		nr.setField(MultiBlocking.BLOCKING_KEY_FIELD, function(record));
		nr.setField(MultiBlocking.BLOCKING_ID_FIELD, getID());
		return nr;
	}
	
	public PactRecord copyWithExplodedBlockingKey(PactRecord record){
		PactRecord nr = record.createCopy();
		nr.setField(MultiBlocking.BLOCKING_KEY_EXTENDED_FIELD, explode(record));
		return nr;
	}
}
