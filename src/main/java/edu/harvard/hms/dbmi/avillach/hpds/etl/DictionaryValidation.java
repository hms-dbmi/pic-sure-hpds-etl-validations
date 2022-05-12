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
							categoriesInDictionary.add(concept_path + "|||" + category);
						}
						for(String category : metaCategories) {
							categoriesInHPDS.add(concept_path + "|||" + category);
						}
					}
				}
			});
		});
		
		SetView<String> missingFromDictionary = Sets.difference(variablesInHPDS, variablesInDictionary);
		System.out.println("In HPDS but not dictionary: " + missingFromDictionary.size());
		missingFromDictionary.forEach((concept_path)->{
			System.out.println("    " + concept_path);
		});
		
		SetView<String> missingFromHPDS = Sets.difference(variablesInDictionary, variablesInHPDS);
		System.out.println("In dictionary but not HPDS : " + missingFromHPDS.size());
		missingFromHPDS.forEach((concept_path)->{
			System.out.println("    " + concept_path);
		});
		
		
		SetView<String> categoriesMissingFromDictionary = Sets.difference(categoriesInHPDS, categoriesInDictionary);
		System.out.println("Missing Categories From Dictionary : " + categoriesMissingFromDictionary.size());
		categoriesMissingFromDictionary.forEach((concept_path)->{
			System.out.println("    " + concept_path);
		});
		
		SetView<String> categoriesMissingFromHPDS = Sets.difference(categoriesInDictionary, categoriesInHPDS);
		System.out.println("Missing Categories From HPDS : " + categoriesMissingFromHPDS.size());
		categoriesMissingFromHPDS.forEach((concept_path)->{
			System.out.println("    " + concept_path);
		});
		
		SetView<String> variablesWithMismatchedType = Sets.union(
				Sets.difference(categoricalVariablesInHPDS, categoricalVariablesInDictionary), 
				Sets.difference(categoricalVariablesInDictionary, categoricalVariablesInHPDS));
		System.out.println("Variables with incosistent type information : " + variablesWithMismatchedType.size());
		variablesWithMismatchedType.forEach((concept_path)->{
			System.out.println("    " + concept_path);
		});
		
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
