package org.semanticweb.semtoo.util;

import java.io.File;
import java.io.PrintWriter;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiomVisitor;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

public class CSVTransform {
	public static void main(String[] args) throws Exception {
		String abox_file = null;
		String abox_dir = null;
		String out_path = "./";
		boolean with_header = true;
		boolean dir_mode = false;
		
		for(int i = 0; i < args.length; i++) {
			if(args[i].equals("-f")) abox_file = args[++i];
			if(args[i].equals("-o")) out_path = args[++i];
//			if(args[i].equals("-nh")) with_header = false;
			if(args[i].equals("-d")) {
				abox_dir = args[++i]; 
				if(!new File(abox_dir).isDirectory()) throw new Exception("Provided path is not a directory");
				dir_mode = true;
			}
		}
		
		if(abox_file == null && abox_dir == null) throw new Exception("No abox file provided");
		
		File out_dir = new File(out_path);
		if(!out_dir.exists() || !out_dir.isDirectory()) out_dir.mkdirs();
		
		
		File ca = new File(out_dir.getAbsolutePath() + "/ca.csv");
		File pa = new File(out_dir.getAbsolutePath() + "/pa.csv");
		
		OWLOntologyManager m = OWLManager.createOWLOntologyManager();
		OWLOntology onto;
		
		PrintWriter ca_writer = new PrintWriter(ca);
		PrintWriter pa_writer = new PrintWriter(pa);
		
		if(with_header) {
			ca_writer.println("\"idv\",\"cls\"");
			pa_writer.println("\"sub\",\"obj\",\"ppt\"");
		}
		
		OWLAxiomVisitor visitor = new OWLAxiomVisitor() {
			@Override
			public void visit(OWLClassAssertionAxiom axiom) {
				OWLIndividual idv = axiom.getIndividual();
				OWLClassExpression exp = axiom.getClassExpression();
				
				if(!(exp instanceof OWLClass)) throw new RuntimeException("Files contains non DL-lite assertion:" + axiom);
				ca_writer.println("\"" + idv.toStringID() + "\",\"" + ((OWLClass)exp).toStringID() + "\"");
			}
			@Override
			public void visit(OWLObjectPropertyAssertionAxiom axiom) {
				OWLIndividual subject = axiom.getSubject();
				OWLIndividual object = axiom.getObject();
				OWLObjectPropertyExpression exp = axiom.getProperty();
				
				if(!(exp instanceof OWLObjectProperty)) throw new RuntimeException("Files contains non DL-lite assertion:" + axiom);
				pa_writer.println("\"" + subject.toStringID() + "\",\"" + object.toStringID() + "\",\"" + ((OWLObjectProperty)exp).toStringID() + "\"");
			}
		};
		
		System.out.println("Transforming, please wait ...");
		
		if(dir_mode) {
			File dir = new File(abox_dir);
		
			for(File f : dir.listFiles()) {
				onto = m.loadOntologyFromOntologyDocument(f);
				onto.axioms().filter(new DLliteFilter("R")).forEach(p -> p.accept(visitor));
			}
		}
		else {
			onto = m.loadOntologyFromOntologyDocument(new File(abox_file));
			onto.axioms().filter(new DLliteFilter("R")).forEach(p -> p.accept(visitor));
		}
		
		System.out.println("Complete!");
		
		ca_writer.close();
		pa_writer.close();
	}

}
