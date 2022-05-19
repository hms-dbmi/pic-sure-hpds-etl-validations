package edu.harvard.hms.dbmi.avillach.hpds.etl;

import java.io.*;
import java.util.Map.Entry;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import edu.harvard.hms.dbmi.avillach.hpds.TopmedDataTable;
import edu.harvard.hms.dbmi.avillach.hpds.TopmedVariable;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;

public class DictionaryValidation {
    private static final String JAVABIN = "/usr/local/docker-config/search/dictionary.javabin";
  
	public static void main(String[] args) throws IOException {
		
		TreeMap<String, ColumnMeta>[] metaStore = new TreeMap[1];
		loadHpdsColumnMeta(metaStore); 
		
		TreeMap<String, TopmedDataTable> dictionary = readDictionary();
		
		Set<String> variablesInHPDS = metaStore[0].keySet();
		Set<String> variablesInDictionary = new TreeSet<String>();
		Set<String> categoricalVariablesInDictionary = new TreeSet<String>();
		Set<String> categoricalVariablesInHPDS = new TreeSet<String>();
		Set<String> categoriesInHPDS = new TreeSet<String>();
		Set<String> categoriesInDictionary = new TreeSet<String>();
		
		dictionary.entrySet().parallelStream().forEach((entry)->{
			List<String> variablesNotInHPDS = new ArrayList<String>();
			entry.getValue().variables.entrySet().forEach((variable)->{
				String concept_path = variable.getValue().getMetadata().get("columnmeta_HPDS_PATH");
				variablesInDictionary.add(concept_path);
				HashMap<String, String> updatedValues = new HashMap<>();
				ColumnMeta variableMeta = metaStore[0].get(variable.getValue().getMetadata().get("columnmeta_HPDS_PATH"));
				if(variable.getValue().isIs_categorical()) {
					categoricalVariablesInDictionary.add(concept_path);
				}
				if(variableMeta!=null) {
					if(variableMeta.isCategorical()) {
						categoricalVariablesInHPDS.add(concept_path);
						Set<String> categories = new TreeSet(variable.getValue().getValues().values());
						Set<String> metaCategories = new TreeSet(variableMeta.getCategoryValues());
						for(String category : categories) {
							categoriesInDictionary.add(
									concept_path + 
									"|||" + category + "|||");
						}
						for(String category : metaCategories) {
							categoriesInHPDS.add(
									concept_path + 
									"|||" + category + "|||");
						}
					}
				}
			});
		});
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File("/usr/local/docker-config/search/validationLog_hpds2dict_concepts.txt")));
		
		System.out.println("Diffing HPDS to dictionary");
		SetView<String> missingFromDictionary = Sets.difference(variablesInHPDS, variablesInDictionary);
		String str = "In HPDS but not dictionary: " + missingFromDictionary.size();
		writeToLog(writer, str);
		missingFromDictionary.forEach((concept_path)->{
			writeToLog(writer, "    " + concept_path);
		});
		writer.close();
		
		BufferedWriter writer2 = new BufferedWriter(new FileWriter(new File("/usr/local/docker-config/search/validationLog_dict2hpds_concepts.txt")));
		System.out.println("Diffing dictionary to HPDS");
		SetView<String> missingFromHPDS = Sets.difference(variablesInDictionary, variablesInHPDS);
		writer2.write("In dictionary but not HPDS : " + missingFromHPDS.size());
		missingFromHPDS.forEach((concept_path)->{
			writeToLog(writer2, "    " + concept_path);
		});
		writer2.close();
		
		BufferedWriter writer3 = new BufferedWriter(new FileWriter(new File("/usr/local/docker-config/search/validationLog_categoriesMissingFromDict.txt")));
		System.out.println("Diffing categories from dictionary");
		SetView<String> categoriesMissingFromDictionary = Sets.difference(categoriesInHPDS, categoriesInDictionary);
		writeToLog(writer3, "Missing Categories From Dictionary : " + categoriesMissingFromDictionary.size());
		categoriesMissingFromDictionary.forEach((concept_path)->{
			writeToLog(writer3, "    " + concept_path);
		});
		writer3.close();

		BufferedWriter writer4 = new BufferedWriter(new FileWriter(new File("/usr/local/docker-config/search/validationLog_categoriesMissingFromHPDS.txt")));
		System.out.println("Diffing categories from HPDS");
		SetView<String> categoriesMissingFromHPDS = Sets.difference(categoriesInDictionary, categoriesInHPDS);
		writeToLog(writer4, "Missing Categories From HPDS : " + categoriesMissingFromHPDS.size());
		categoriesMissingFromHPDS.forEach((concept_path)->{
			writeToLog(writer4, "    " + concept_path);
		});
		writer4.close();

		BufferedWriter writer5 = new BufferedWriter(new FileWriter(new File("/usr/local/docker-config/search/validationLog_mismatchedTypes.txt")));
		System.out.println("Mismatched types");
		SetView<String> variablesWithMismatchedType = Sets.union(
				Sets.difference(categoricalVariablesInHPDS, categoricalVariablesInDictionary), 
				Sets.difference(categoricalVariablesInDictionary, categoricalVariablesInHPDS));
		writeToLog(writer5, "Variables with incosistent type information : " + variablesWithMismatchedType.size());
		variablesWithMismatchedType.forEach((concept_path)->{
			writeToLog(writer5, "    " + concept_path);
		});
		writer5.close();
	}

	private static void writeToLog(BufferedWriter writer, String str){
		try{
			writer.write(str);
			writer.newLine();
			writer.flush();
		}catch(Exception e) {
			
		}
	}

	private static void loadHpdsColumnMeta(TreeMap<String, ColumnMeta>[] metaStore) {
		try (ObjectInputStream objectInputStream = new ObjectInputStream(new GZIPInputStream(new FileInputStream("/opt/local/hpds/columnMeta.javabin")));){
			TreeMap<String, ColumnMeta> _metastore = (TreeMap<String, ColumnMeta>) objectInputStream.readObject();
			TreeMap<String, ColumnMeta> metastoreScrubbed = new TreeMap<String, ColumnMeta>();
			for(Entry<String,ColumnMeta> entry : _metastore.entrySet()) {
				metastoreScrubbed.put(entry.getKey().replaceAll("\\ufffd",""), entry.getValue());
			}
			metaStore[0] = metastoreScrubbed;
			objectInputStream.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private static TreeMap<String, TopmedDataTable> readDictionary() {
        try(ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(JAVABIN)));){
            return (TreeMap<String, TopmedDataTable>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return new TreeMap<>();
    }	
}
