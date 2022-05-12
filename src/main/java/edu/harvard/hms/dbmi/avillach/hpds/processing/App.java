package edu.harvard.hms.dbmi.avillach.hpds.processing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;

import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;

public class App 
{
	protected static LoadingCache<String, PhenoCube<?>> store;

	public static void main( String[] args ) throws IOException, ClassNotFoundException
	{
		QueryProcessor processor = new QueryProcessor();
		HashMap<String, Object> validationData = new HashMap<>();
		for(File file : new File("/usr/local/docker-config/dbgap_validation_data").listFiles()) {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			String studyAccession = null;
			String tableAccession = null;
			String consentGroup = null;
			String[] phvList = null;
			while(line.startsWith("#") || line.isBlank()){
				if(line.isBlank()) {
					line = reader.readLine();
					continue;
				}
				if(line.startsWith("# Study accession:")) {
					studyAccession = line.split(":")[1].trim();
				}
				if(line.startsWith("# Table accession:")) {
					tableAccession = line.split(":")[1].trim();
				}
				if(line.startsWith("# Consent group:")) {
					consentGroup = line.split(":")[1].trim();
				}
				if(line.startsWith("##")) {
					phvList = line.split("\t");
				}
				line = reader.readLine();
			}

			if(studyAccession==null) {
				studyAccession = file.getName().split("\\.")[0];
				tableAccession = file.getName().split("\\.")[2];
			}
			if(phvList==null) {
				System.out.println(file.getName());
			}

			String[] headers = line.split("\t");
			HashMap<String, Integer>[] valueCounts = new HashMap[headers.length];
			for(int x = 0;x<valueCounts.length;x++) {
				valueCounts[x] = new HashMap<String, Integer>();
			}
			line = reader.readLine();
			while(line!=null) {
				List<String> fields = Arrays.asList(line.split("\t"));
				for(int x = 0;x<fields.size();x++) {
					Integer currentCountForValue = valueCounts[x].get(fields.get(x));
					if(currentCountForValue == null) {
						currentCountForValue = 1;
						valueCounts[x].put(fields.get(x), currentCountForValue);
					}else {
						currentCountForValue = currentCountForValue+1;
						valueCounts[x].put(fields.get(x), currentCountForValue);
					}
				}
				line = reader.readLine();
			}
			HashMap<String, Object> fileMeta = new HashMap<String, Object>();
			fileMeta.put("StudyAccession", studyAccession);
			fileMeta.put("TableAccession", tableAccession);
			fileMeta.put("ConsentGroup", consentGroup);
			HashMap<String, Object> validationResults = new HashMap<String, Object>();
			fileMeta.put("ValidationSummary", validationResults);
			for(int x = 0;x<valueCounts.length;x++) {
				String conceptPath = createConceptPath(studyAccession, tableAccession, phvList, headers, x);
				PhenoCube cube = processor.getCube(conceptPath);
				if(!isLikelyContinuous(valueCounts[x])) {
					validationResults.put(conceptPath, valueCounts[x]);
					TreeMap<String, List> categoryMap = cube.getCategoryMap();
					ArrayList<Entry<String, List>> categoryEntries = new ArrayList(categoryMap.entrySet());
					Collections.sort(categoryEntries, (Entry<String,List> a, Entry<String, List> b)->{
						return a.getValue().size() - b.getValue().size();
					});

				}else {
					DoubleSummaryStatistics dss = new DoubleSummaryStatistics();
					for(String value : valueCounts[x].keySet()) {
						String valueTrimmed = value.trim();
						if(!valueTrimmed.isBlank() && !valueTrimmed.contentEquals("NA")) {
							dss.accept(Double.parseDouble(valueTrimmed));						
						}
					}
					validationResults.put(conceptPath, dss);
				}
			}
			validationData.put(file.getName(),fileMeta);
		}
		String validationDataString = new ObjectMapper().writeValueAsString(validationData);
		System.out.println(validationDataString);

		for(String filename : validationData.keySet()) {
			Map<String, Map<String, Object>> validationDataForFile = (Map<String, Map<String,Object>>)(validationData.get(filename));
			for(String concept_path : validationDataForFile.keySet()) {
				PhenoCube cube = processor.getCube("concept_path");
				Map<String, Object> validationDataForConcept = validationDataForFile.get(concept_path);
				if(cube.isStringType()) {
					// If the cube is string type that means categorical data.
					// We have to assume that our analysis of the data type going to be of the categorical type
					// If it is not, that is something that has to be looked into manually to identify which side is wrong

				} else {
					// If the cube is not string type that should mean continuous data.
					System.out.println("Checking concept : "  + concept_path);
					DoubleSummaryStatistics dss = new DoubleSummaryStatistics();
					for(Comparable value : Arrays.stream(cube.sortedByValue()).map((KeyAndValue keyAndValue)->{return keyAndValue.getValue();}).collect(Collectors.toList())) {
						dss.accept(((Double)value));						
					}
					if(dss.getAverage()!=Double.parseDouble((String)validationDataForConcept.get("average"))){
						System.out.println("    Average differs: " + dss.getAverage() + " - " + Double.parseDouble((String)validationDataForConcept.get("average")));
					}
					if(dss.getMin()!=Double.parseDouble((String)validationDataForConcept.get("min"))){
						System.out.println("    Minimum differs: " + dss.getMin() + " - " + Double.parseDouble((String)validationDataForConcept.get("min")));
					}
					if(dss.getMax()!=Double.parseDouble((String)validationDataForConcept.get("max"))){
						System.out.println("   Maximum differs: " + dss.getMax() + " - " + Double.parseDouble((String)validationDataForConcept.get("max")));
					}
					if(dss.getSum()!=Double.parseDouble((String)validationDataForConcept.get("sum"))){
						System.out.println("   Sum differs: " + dss.getSum() + " - " + Double.parseDouble((String)validationDataForConcept.get("sum")));
					}
				}
			}
		}
	}

	private static String createConceptPath(String studyAccession, String tableAccession, String[] phvList,
			String[] headers, int x) {
		return "\\"+ 
				studyAccession.split("\\.")[0]+"\\"+
				tableAccession.split("\\.")[0]+"\\"+
				(x>0?phvList[x].split("\\.")[0]+"\\":"")+
				headers[x]+"\\";
	}

	private static boolean isLikelyContinuous(HashMap<String, Integer> valueCounts) {
		return (
				allValuesAreDoubles(valueCounts)
				&& !allValuesAreIntegers(valueCounts)
				);
	}

	private static boolean allValuesAreDoubles(HashMap<String, Integer> valueCounts) {
		return valueCounts.keySet().stream().filter((value)->{
			return isParseableDouble(value) || value.contentEquals("NA") || value.trim().equals("");
		}).collect(Collectors.toList()).size() == valueCounts.size();
	}

	private static boolean isParseableDouble(String value) {
		boolean ret = false;
		try{
			Double.parseDouble(value.trim());
			ret = true;
		}catch(Exception e) {

		}
		return ret;
	}

	private static boolean allValuesAreIntegers(HashMap<String, Integer> valueCounts) {
		return valueCounts.keySet().stream().filter((value)->{
			return value.isBlank() || value.contentEquals("NA") || isParseableInteger(value);
		}).collect(Collectors.toList()).size() == valueCounts.size();
	}

	private static boolean isParseableInteger(String value) {
		boolean ret = false;
		try{
			Integer.parseInt(value.trim());
			ret = true;
		}catch(Exception e) {

		}
		return ret;
	}
}
