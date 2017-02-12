package org.semanticweb.semtoo.imports;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.semanticweb.semtoo.embeddedneo4j.StDatabaseBuilder;
import org.semanticweb.semtoo.embeddedneo4j.StDatabaseMeta.node_labels;
import org.semanticweb.semtoo.embeddedneo4j.StDatabaseMeta.property_key;

public class CSVImporter {
	private GraphDatabaseService embeddedDB;
	
	public CSVImporter(GraphDatabaseService _db) {
		embeddedDB = _db;
	}
	
	public void import_classAssertion_csv(String clsAssertioncsv) throws IOException {
		clsAssertioncsv = new File(clsAssertioncsv).getCanonicalPath().replace("\\\\", "/");
		
		String cypher1 = "USING PERIODIC COMMIT 1000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + clsAssertioncsv + "\" AS clsa"
				+ " MATCH (cls:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":clsa.class})"
				+ " MERGE (idv:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":clsa.idv})"
				+ " CREATE (idv)-[:is]->(cls)";
		
		
		long start, end;
		System.out.println("Begin inserting class assertion ...");
		start = System.currentTimeMillis();
		embeddedDB.execute(cypher1);
		end = System.currentTimeMillis();
		System.out.println("Done class assertion with " + (end - start) + " ms");
	}
	
	public void import_propertyAssertion_csv(String pptAssertincsv) throws IOException {
		pptAssertincsv = new File(pptAssertincsv).getCanonicalPath().replace("\\\\", "/");
		
//		String cypher2 = "USING PERIODIC COMMIT 2000"
//				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
//				+ " MERGE (a:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject})"
//				+ " MERGE (ab:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject + ppta.object})"
//				+ " ON CREATE SET ab." + property_key.SUBJECT_IRI + "=ppta.subject, ab." + property_key.OBJECT_IRI + "=ppta.object";
//		
//		String cypher2_1 = "USING PERIODIC COMMIT 2000"
//				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
//				+ " MERGE (b:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object})"
//				+ " MERGE (ba:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object + ppta.subject})"
//				+ " ON CREATE SET ba." + property_key.SUBJECT_IRI + "=ppta.object, ba." + property_key.OBJECT_IRI + "=ppta.subject";
		
		String cypher2 = "USING PERIODIC COMMIT 1000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
				+ " MERGE (a:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject})"
				+ " MERGE (b:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object})"				
				+ " MERGE (ab:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject + ppta.object})"
//				+ " ON CREATE SET ab." + property_key.SUBJECT_ID + "= id(a), ab." + property_key.OBJECT_ID + "= id(b)"
				+ " MERGE (ba:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object + ppta.subject})";
//				+ " ON CREATE SET ba." + property_key.SUBJECT_ID + "= id(b), ba." + property_key.OBJECT_ID + "= id(a)";
		
		String cypher3 = "USING PERIODIC COMMIT 1000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
				+ " MATCH (p:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ": ppta.property})"
				+ " MATCH (ab:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject + ppta.object})"
				+ " MATCH (rp:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":\"prt_\" + ppta.property})"
				+ " MATCH (a:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject})"
				+ " CREATE (ab)-[r1:is]->(p)"
				+ " CREATE (a)-[r3:is {" + property_key.CAUSES + ":id(r1)}]->(rp)";
//				+ " ON CREATE SET r3." + property_key.CAUSES + " = [id(ab)]"
//				+ " ON MATCH SET r3." + property_key.CAUSES + " = r3." + property_key.CAUSES + " + id(ab)";
		
		String cypher3_1 = "USING PERIODIC COMMIT 1000"
				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
				+ " MATCH (ip:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":\"inv_\" + ppta.property})"
				+ " MATCH (ba:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object + ppta.subject})"
				+ " MATCH (rip:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":\"prt_inv_\" + ppta.property})"
				+ " MATCH (b:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object})"				
				+ " CREATE (ba)-[r2:is]->(ip)"
				+ " CREATE (b)-[r4:is {" + property_key.CAUSES + ":id(r2)}]->(rip)";
		//		+ " ON CREATE SET r4." + property_key.CAUSES + " = [id(ba)]"
		//		+ " ON MATCH SET r4." + property_key.CAUSES + " = r4." + property_key.CAUSES + " + id(ba)";
//		String cypher3 = "USING PERIODIC COMMIT 2000"
//				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
//				+ " MATCH (p:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ": ppta.property})"
//				+ " MATCH (ip:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":\"inv_\" + ppta.property})"
//				+ " MATCH (ab:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject + ppta.object})"
//				+ " MATCH (ba:" + node_labels.DUALINDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object + ppta.subject})"
//				+ " CREATE (ab)-[:is]->(p)"
//				+ " CREATE (ba)-[:is]->(ip)";
//		
//		String cypher3_1 = "USING PERIODIC COMMIT 2000"
//				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
//				+ " MATCH (rp:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":\"prt_\" + ppta.property})"
//				+ " MATCH (a:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.subject})"
//				+ " MERGE (a)-[:is]->(rp)";
//		
//		String cypher3_2 = "USING PERIODIC COMMIT 2000"
//				+ " LOAD CSV WITH HEADERS FROM \"file:///" + pptAssertincsv + "\" AS ppta"
//				+ " MATCH (rip:" + node_labels.TBOXENTITY + " {" + property_key.IRI_LOWER + ":\"prt_inv_\" + ppta.property})"
//				+ " MATCH (b:" + node_labels.INDIVIDUAL + " {" + property_key.NODE_IRI + ":ppta.object})"
//				+ " MERGE (b)-[:is]->(rip)";
		
		System.out.println(cypher2);
		System.out.println(cypher3);
		System.out.println(cypher3_1);
		long start, end;
		System.out.println("Begin inserting property assertion ...");
		start = System.currentTimeMillis();
		
		embeddedDB.execute(cypher2);
		
		end = System.currentTimeMillis();
		System.out.println("Done cypher2 with " + (end - start) + " ms");
		
//		embeddedDB.execute(cypher2_1);
//		
//		end = System.currentTimeMillis();
//		System.out.println("Done cypher2_1 with " + (end - start) + " ms");
		
		embeddedDB.execute(cypher3);
		
		end = System.currentTimeMillis();
		System.out.println("Done cypher3 with " + (end - start) + " ms");
		
		embeddedDB.execute(cypher3_1);
		
		end = System.currentTimeMillis();
		System.out.println("Done cypher3_1 with " + (end - start) + " ms");
//		
////		embeddedDB.execute(cypher3_2);
//		
//		end = System.currentTimeMillis();
//		System.out.println("Done cypher3_2 and Complete with " + (end - start) + " ms");
	}
}
