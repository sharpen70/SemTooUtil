package org.semanticweb.semtoo.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.semtoo.imports.CSVImporter;
import org.semanticweb.semtoo.imports.OWLTransfer;

public class Neo4jImporter {

	public static void main(String[] args) throws Exception {
		String db_loc = "./default.db";
		String onto_path = null;
		
		boolean import_csv = false;
		List<String> csvs = new ArrayList<>();
		
		for(int i = 0; i < args.length; i++) {
			if(args[i].equals("-d")) db_loc = args[++i];
			if(args[i].equals("-csv")) {
				import_csv = true;
				
				while(i + 1 < args.length && !(args[i + 1].startsWith("-"))) {
					csvs.add(args[++i]);
				}
			}
			if(args[i].equals("-onto")) onto_path = args[++i];
		}
		
		if(!import_csv) {
			if(onto_path == null) {
				System.out.println("Require ontology file"); 
				return;
			}
			
			File onto = new File(onto_path);
			
			OWLOntologyManager man = OWLManager.createOWLOntologyManager();
			OWLOntology o = man.loadOntologyFromOntologyDocument(onto);
			
			File _db = new File(db_loc);
			if(_db.exists()) {
				System.out.println("Clean the original database ...");
				FileUtils.deleteDirectory(_db);
			}
			
			GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(db_loc))
					.setConfig(GraphDatabaseSettings.pagecache_memory, "6g").newGraphDatabase();
			
			OWLTransfer owltf = new OWLTransfer(o, dbservice);
			owltf.loadOntologyToGraph();
			
			dbservice.shutdown();
		}
		else {
			if(csvs.size() == 0) {
				System.out.println("No csv files"); 
				return;
			}
			
			GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(db_loc))
					.setConfig(GraphDatabaseSettings.pagecache_memory, "6g").newGraphDatabase();
			
			CSVImporter importer = new CSVImporter(dbservice);
			
			for(String s: csvs) {
				File f = new File(s);
				Scanner scanner = new Scanner(f);
				String h = scanner.nextLine();
				
				if(h.split(",").length == 2) importer.import_classAssertion_csv(s);
				if(h.split(",").length == 3) importer.import_propertyAssertion_csv(s);
				
				scanner.close();
			}
			
			dbservice.shutdown();
		}
	}
}
