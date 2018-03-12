import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class webParser//website data parser
         {
    private static  Pattern namePattern;
    private static  Pattern linkPattern;
    static{
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }
    private int numSetReducers;

    public static String[] parse(String value) {
        if (!value.toString().equals("")) {
            try {
                // Configure parser.
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                SAXParser saxParser = spf.newSAXParser();
                XMLReader xmlReader = saxParser.getXMLReader();
                // Parser fills this list with linked page names.
                List<String> linkPageNames = new LinkedList<>();
                xmlReader.setContentHandler(new WikiParser(linkPageNames));


                String line = value.toString();
                for (int i = 0; i < 1; i++) {  //exactly one loop since there is only one line
                    // Each line formatted as (Wiki-page-name:Wiki-page-html).
                    int delimLoc = line.indexOf(':');
                    String pageName = line.substring(0, delimLoc);
                    String html = line.substring(delimLoc + 1);
                    Matcher matcher = namePattern.matcher(pageName);
                    if (!matcher.find()) {
                        // Skip this html file, name contains (~).
                        continue;
                    }

                    // Parse page and fill list of linked pages.
                    linkPageNames.clear();
                    try {
                        xmlReader.parse(new InputSource(new StringReader(html)));
                    } catch (Exception e) {
                        String[] result={pageName};
                       return result;//no link

                        // do not Discard ill-formatted pages. emit instead ~~~meaning None
                    }

                    // always print the page and its links.
                    if (linkPageNames.size() != 0) {
                        String[] result=new String[linkPageNames.size()+1];
                        result[0]=pageName;
                        int j=1;
                        for (String val : linkPageNames) {
                            result[j]=val;
                            j+=1;
                        }
                        return result;
                    } else { //no link
                        String[] result={pageName};
                        return result;
                    }

                }

            } catch (Exception e) {
                e.printStackTrace();
            }//indicating there should be no such value in RDD

        }
        return new String[0];//indicating there should be no such value in RDD
    }
    /** Parses a Wikipage, finding links inside bodyContent div element.*/
    private static class WikiParser extends DefaultHandler {
        /** List of linked pages; filled by parser. */
        private List<String> linkPageNames;
        /** Nesting depth inside bodyContent div element. */
        private int count = 0;

        public WikiParser(List<String> linkPageNames) {
            super();
            this.linkPageNames = linkPageNames;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
                // Beginning of bodyContent div element.
                count = 1;
            } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
                // Anchor tag inside bodyContent div element.
                count++;
                String link = attributes.getValue("href");
                if (link == null) {
                    return;
                }
                try {
                    // Decode escaped characters in URL.
                    link = URLDecoder.decode(link, "UTF-8");
                } catch (Exception e) {
                    // Wiki-weirdness;
                }
                // Keep only html filenames ending relative paths and not containing tilde (~).
                Matcher matcher = linkPattern.matcher(link);
                if (matcher.find()) {
                    linkPageNames.add(matcher.group(1));
                }
            } else if (count > 0) {
                // Other element inside bodyContent div.
                count++;
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (count > 0) {
                // End of element inside bodyContent div.
                count--;
            }
        }
    }

}
