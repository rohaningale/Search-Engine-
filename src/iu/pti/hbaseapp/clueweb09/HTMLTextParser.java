package iu.pti.hbaseapp.clueweb09;

import java.io.StringReader;

import org.apache.xerces.dom.CoreDocumentImpl;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/*
* HTMLTextParser.java
* Author: S.Prasanna
*
*/

public class HTMLTextParser {
	StringBuffer TextBuffer = null;
	InputSource inSource = null;

	// Gets the text content from Nodes recursively
	void processNode(Node node) {
		if (node == null)
			return;

		// Process a text node
		if (node.getNodeType() == Node.TEXT_NODE) {
			String txt = node.getNodeValue().trim();
			if ( txt.length() > 0 && !(txt.startsWith("<!--") && txt.endsWith("-->")) && !(txt.startsWith("/*") && txt.endsWith("*/")) ) {
				TextBuffer.append(txt).append('\n');
			}
		} else if (node.hasChildNodes()) {
			// Process the Node's children
			NodeList childList = node.getChildNodes();
			int childLen = childList.getLength();
			for (int count = 0; count < childLen; count++)
				processNode(childList.item(count));
		} else
			return;
	}

	// Extracts text from HTML Document
	public String htmltoText(String htmlContent) {

		DOMFragmentParser parser = new DOMFragmentParser();

		try {
			inSource = new InputSource(new StringReader(htmlContent) );
		} catch (Exception e) {
			System.out.println("Unable to open Input source from string " + htmlContent);
			return null;
		}

		CoreDocumentImpl codeDoc = new CoreDocumentImpl();
		DocumentFragment doc = codeDoc.createDocumentFragment();

		try {
			parser.parse(inSource, doc);
		} catch (Exception e) {
			System.out.println("Unable to parse HTML file!");
			return null;
		}

		TextBuffer = new StringBuffer();

		// Node is a super interface of DocumentFragment, so no typecast needed
		processNode(doc);

		return TextBuffer.toString();
	}
}
