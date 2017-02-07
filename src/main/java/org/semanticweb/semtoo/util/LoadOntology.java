package org.semanticweb.semtoo.util;

import java.io.File;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.semtoo.graph.GraphManager;
import org.semanticweb.semtoo.neo4j.Neo4jManager;


public class LoadOntology {
	public static final String default_owl = "http://swat.cse.lehigh.edu/onto/univ-bench.owl";
	
	public static void main(String[] args) throws Exception {
//			if(args.length < 2) throw new Exception("Need at least two variables, the first OWLOnotlogy file, the second Abox DB file.");
	
		String owl_path = default_owl;
		boolean fromfile = false;
		
		for(int i = 0; i < args.length; i += 2) {
			if(args[i].equals("-onto")) owl_path = args[i + 1];
			if(args[i].equals("-f"))  {
				fromfile = true;
				owl_path = args[i + 1];
			}
		}
		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology;
		
		if(!fromfile) ontology = manager.loadOntologyFromOntologyDocument(IRI.create(owl_path));	
		else ontology = manager.loadOntologyFromOntologyDocument(new File(owl_path));
		
		GraphManager gm = new GraphManager();
		gm.clearGraph();
		gm.loadOntologyToGraph(ontology);
		
		Neo4jManager m = Neo4jManager.getManager();
		
		try(Session session = m.getSession()) {
			try(Transaction tx = session.beginTransaction()) {
				String statement = "MATCH (n) MATCH (a)-[r]-(b) return count(DISTINCT n) as node, count(DISTINCT r) as rel";
				StatementResult result = tx.run(statement);
				Record r = result.next();
				System.out.println(r.get("node") + " nodes added, " + r.get("rel") + " relationships added");
			}
		}
	}
}
