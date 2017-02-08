package org.semanticweb.semtoo.imports;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.semanticweb.semtoo.embeddedneo4j.SemtooDatabase;
import org.semanticweb.semtoo.embeddedneo4j.SemtooDatabaseMeta.node_labels;
import org.semanticweb.semtoo.embeddedneo4j.SemtooDatabaseMeta.property_key;

public class CSVImporter {
	private SemtooDatabase db;
	
	public CSVImporter(SemtooDatabase _db) {
		db = _db;
	}
	
	public void import_classAssertion_csv(String clsAssertioncsv) throws IOException {
		clsAssertioncsv = new File(clsAssertioncsv).getCanonicalPath().replace("\\\\", "/");
		
		String cypher1 = "USING PERIODIC COMMIT 2000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + clsAssertioncsv + "\" AS clsa"
				+ " MATCH (cls:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":clsa.class})"
				+ " MERGE (idv:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":clsa.idv})"
				+ " CREATE (idv)-[:is]->(cls)";
		
		GraphDatabaseService embeddedDB = db.getEmbeddedDB();
		
		long start, end;
		System.out.println("Begin inserting class assertion ...");
		start = System.currentTimeMillis();
		embeddedDB.execute(cypher1);
		end = System.currentTimeMillis();
		System.out.println("Done class assertion with " + (end - start) + " ms");
	}
	
	public void import_propertyAssertion_csv(String pptAssertincsv) throws IOException {
		pptAssertincsv = new File(pptAssertincsv).getCanonicalPath().replace("\\\\", "/");
		
		GraphDatabaseService embeddedDB = db.getEmbeddedDB();
		
		String cypher2 = "USING PERIODIC COMMIT 2000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
				+ " MERGE (a:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject})"
				+ " MERGE (ab:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject + ppta.object})"
				+ " ON CREATE SET ab." + property_key.SUBJECT_IRI + "=ppta.subject, ab." + property_key.OBJECT_IRI + "=ppta.object";
		
		String cypher2_1 = "USING PERIODIC COMMIT 2000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
				+ " MERGE (b:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object})"
				+ " MERGE (ba:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object + ppta.subject})"
				+ " ON CREATE SET ba." + property_key.SUBJECT_IRI + "=ppta.object, ba." + property_key.OBJECT_IRI + "=ppta.subject";
		
		String cypher3 = "USING PERIODIC COMMIT 2000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
				+ " MATCH (p:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ": ppta.property})"
				+ " MATCH (ip:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":\"inv_\" + ppta.property})"
				+ " MATCH (ab:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject + ppta.object})"
				+ " MATCH (ba:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object + ppta.subject})"
				+ " CREATE (ab)-[:is]->(p)"
				+ " CREATE (ba)-[:is]->(ip)";
		
		String cypher3_1 = "USING PERIODIC COMMIT 2000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
				+ " MATCH (rp:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":\"prt_\" + ppta.property})"
				+ " MATCH (a:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject})"
				+ " MERGE (a)-[:is]->(rp)";
		
		String cypher3_2 = "USING PERIODIC COMMIT 2000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
				+ " MATCH (rip:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":\"prt_inv_\" + ppta.property})"
				+ " MATCH (b:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object})"
				+ " MERGE (b)-[:is]->(rip)";
		
		long start, end;
		System.out.println("Begin inserting property assertion ...");
		start = System.currentTimeMillis();
		
		embeddedDB.execute(cypher2);
		
		end = System.currentTimeMillis();
		System.out.println("Done cypher2 with " + (end - start) + " ms");
		
		embeddedDB.execute(cypher2_1);
		
		end = System.currentTimeMillis();
		System.out.println("Done cypher2_1 with " + (end - start) + " ms");
		
		embeddedDB.execute(cypher3);
		
		end = System.currentTimeMillis();
		System.out.println("Done cypher3 with " + (end - start) + " ms");
		
		embeddedDB.execute(cypher3_1);
		
		end = System.currentTimeMillis();
		System.out.println("Done cypher3_1 with " + (end - start) + " ms");
		
		embeddedDB.execute(cypher3_2);
		
		end = System.currentTimeMillis();
		System.out.println("Done cypher3_2 and Complete with " + (end - start) + " ms");
	}
}
