import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import javax.xml.bind.DatatypeConverter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class DataSetProcessor {

  public static String COLUMN_FAMILY = "p";
  /*
  public static final String[] URL_PREFIXES = {"" + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "<http://www.w3.org/2000/01/rdf-schema#",
    "<http://xmlns.com/foaf/0.1/",
    "<http://purl.org/dc/elements/1.1/",
    "<http://www.w3.org/2001/XMLSchema#",
    "<http://purl.org/stuff/rev#",
    "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/",
    "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/"};

  public static final String[] URL_ABBREVIATIONS = {"rdf_", "rdfs_", "foaf_", "dc_", "xsd_", "rev_",
    "bsbm-voc_", "bsbm-inst_"};
  */
  public static final String[] URL_ABBREVIATIONS = {
			"<http://dbpedia.org/resource/",
			"<http://dbpedia.org/property/",
			"<http://dbpedia.org/ontology/",
			"<http://dbpedia.org/class/",
			"<http://www.w3.org/",
			"<http://xmlns.com/foaf/0.1/"
	};
	
  public static final String[] URL_PREFIXES = {"res_","prop_","ont_","class_","w3_","foaf_"};

  public static final Pattern pattern = Pattern.compile("([^\"]\\S*|\".+?\")\\s*");
  public static final Pattern valuePattern = Pattern.compile("^^", Pattern.LITERAL);
  public static final TypeParser defaultParser = new StringParser();

  private static ImmutableMap<String, TypeParser> typesToParsers;
  

  public static final Triple process(String text) {

    Matcher matcher = pattern.matcher(text);
    if (matcher.groupCount() > 4) {
      throw new IllegalStateException(text);
    }

    matcher.find();
    String subject = matcher.group(1);
    matcher.find();
    String predicate = matcher.group(1);
    matcher.find();

    // if the "object" portion starts with a "<" then it is actually an object, we can return
    String nextComponent = matcher.group(1);
    if (nextComponent.startsWith("<")) {
      return new Triple(subject, predicate, nextComponent);
    }

    String value = nextComponent.replace("\"", "");
    //else it maybe a comment or a value, we treat comments as string values
    matcher.find();
    String next = matcher.group(1);
    String[] split = valuePattern.split(next);
    if (split.length == 2) {
      TypeParser parser = typesToParsers.get(split[1]);
      if (parser != null) {
        return new Triple(subject, predicate, value, parser);
      }
    }

    // just treat the value as a string literal if everything else fails
    return new Triple(subject, predicate, value, defaultParser);
  }

	private static String removeBannedChars(String s) {
		String BANNED_CHARACTERS = "[-\\<>:#//@%&().]";
		return s.replace(">", "").replaceAll(BANNED_CHARACTERS, "_");
	}

  private static String replaceWithShortName(String original) {
    for (int i = 0; i < URL_PREFIXES.length; i++) {
      if (original.contains(URL_PREFIXES[i])) {
        return removeBannedChars(original.replace(URL_PREFIXES[i], URL_ABBREVIATIONS[i]).replace(">", ""));
      }
    }
    return original;
  }

  public static class Triple {

    public final String subject;
    public final String predicate;
    public final String object;
    public final byte[] value;
    public final Object rawValue;

    private Triple(String subject, String predicate, String object) {
      this.subject = replaceWithShortName(subject);
      this.predicate = replaceWithShortName(predicate);
      this.object = replaceWithShortName(object);
      this.value = null;
      this.rawValue = null;
    }

    private Triple(String subject, String predicate, String rawValue, TypeParser parser) {
      this.subject = replaceWithShortName(subject);
      this.predicate = replaceWithShortName(predicate);
      this.object = null;
      this.value = parser.parse(rawValue);
      this.rawValue = rawValue;
    }

    public boolean isValueTriple() {
      return this.value != null;
    }

    public String toString() {
      return Objects.toStringHelper(this)
        .add("subject", subject)
        .add("predicate", predicate)
        .add("object", object)
        .add("rawValue", rawValue)
        .toString();
    }
  }


  private interface TypeParser {
    public byte[] parse(String input);
  }

  private static class IntParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Integer.parseInt(input));
    }
  }

  private static class DoubleParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Double.parseDouble(input));
    }
  }

  @SuppressWarnings("unused")
private static class ShortParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Short.parseShort(input));
    }
  }

  private static class FloatParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Float.parseFloat(input));
    }
  }

  @SuppressWarnings("unused")
private static class LongParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Long.parseLong(input));
    }
  }

  @SuppressWarnings("unused")
private static class ByteParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Byte.parseByte(input));
    }
  }

  @SuppressWarnings("unused")
private static class BooleanParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(Boolean.parseBoolean(input));
    }
  }

  private static class DateParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(DatatypeConverter.parseDate(input).getTime().getTime());
    }
  }

  private static class DateTimeParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return toBytes(DatatypeConverter.parseDateTime(input).getTime().getTime());
    }
  }

  private static class StringParser implements TypeParser {
    @Override
    public byte[] parse(String input) {
      return input.getBytes();
    }
  }

  //  public static void main(String[] args) throws IOException {
//    BSBMToHBaseMapper mapper = new BSBMToHBaseMapper();
//    BufferedReader reader = new BufferedReader(new FileReader(args[0]));
//    String line;
//    while ((line = reader.readLine()) != null) {
//      Triple triple = mapper.process(line);
//      System.out.println(triple);
//    }
//  }
  
  /*
  For BSBM
  static {
    ImmutableMap.Builder<String, TypeParser> typesToParsersBuilder = ImmutableMap.builder();
    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#date>", new DateParser());
    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#dateTime>", new DateTimeParser());
    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#string>", new StringParser());
    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#integer>", new IntParser());
    typesToParsersBuilder.put("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/USD>", new DoubleParser());
    typesToParsers = typesToParsersBuilder.build();
  }
  */
  // For DBpedia
  static {
	    ImmutableMap.Builder<String, TypeParser> typesToParsersBuilder = ImmutableMap.builder();
	    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#int>", new StringParser());
	    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#double>", new StringParser());
	    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#gYear>", new StringParser());
	    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#anyURI>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/minute>", new StringParser());
	    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#integer>", new StringParser());
	    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#date>", new StringParser());
	    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#float>", new StringParser());
	    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#gMonthDay>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilometre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/rod>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/usDollar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/centimetre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/second>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/foot>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/astronomicalUnit>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/day>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/stone>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/squareKilometre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/acre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/hectare>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/mile>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicCentimetre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilogram>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/nautialMile>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/newtonMetre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/engineConfiguration>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilowatt>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/hour>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/inch>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/pound>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/megahertz>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/imperialGallon>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/litre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/millimetre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/nicaraguanC??rdoba>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/euro>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilonewton>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/metre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/byte>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/knot>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/degreeRankine>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/squareMetre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicMetre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/squareMile>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/pond>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/hectolitre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/gram>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilometrePerHour>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/horsepower>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/milePerHour>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/watt>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/newton>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/squareFoot>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/footPerMinute>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/metrePerSecond>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilohertz>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/chileanPeso>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kelvin>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilopascal>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/nanometre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/tonne>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/meganewton>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicFoot>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicMetrePerSecond>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/footPerSecond>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/yard>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/valvetrain>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/swedishKrona>", new StringParser());
	    typesToParsersBuilder.put("<http://www.w3.org/2001/XMLSchema#gMonth>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicKilometre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/poundSterling>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/gigametre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilometrePerSecond>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/iranianRial>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/usGallon>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/degreeFahrenheit>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/renminbi>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilogramForce>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/albanianLek>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/joule>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/megawatt>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/gramPerCubicCentimetre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/pferdestaerke>", new StringParser());
	    typesToParsersBuilder.put("http://dbpedia.org/datatype/brake_horsepower", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/norwegianKrone>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/ounce>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/millilitre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/tonganPaanga>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/millihertz>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/inhabitantsPerSquareKilometre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/fuelType>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/japaneseYen>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/swissFranc>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/australianDollar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/indianRupee>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/omaniRial>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/gigabyte>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/georgianLari>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/canadianDollar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/bermudianDollar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicMile>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/sriLankanRupee>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/mauritianRupee>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/bhutaneseNgultrum>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/southKoreanWon>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/latvianLats>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicFeetPerSecond>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/megalitre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/hungarianForint>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/danishKrone>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/degreeCelsius>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/slovakKoruna>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/pakistaniRupee>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/czechKoruna>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/argentinePeso>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/maldivianRufiyaa>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/liberianDollar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/giganewton>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/megabyte>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/jordanianDinar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/philippinePeso>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/papuaNewGuineanKina>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/gramPerKilometre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/poundPerSquareInch>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilometresPerLitre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/saudiRiyal>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/guineaFranc>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/gigalitre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/ethiopianBirr>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/bolivianBoliviano>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/namibianDollar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/polishZ??oty>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/somaliShilling>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/armenianDram>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/kilowattHour>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/turkishLira>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/croatianKuna>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/moroccanDirham>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/grain>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/saintHelenaPound>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/hongKongDollar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/bangladeshiTaka>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/singaporeDollar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/thaiBaht>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/ukrainianHryvnia>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/nepaleseRupee>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/russianRouble>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/belizeDollar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicDecametre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/panamanianBalboa>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/falklandIslandsPound>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/malaysianRinggit>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/nigerianNaira>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/serbianDinar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/millibar>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/seychellesRupee>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/belarussianRuble>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/eritreanNakfa>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/peruvianNuevoSol>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicHectometre>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/netherlandsAntilleanGuilder>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/lithuanianLitas>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/terabyte>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/myanmaKyat>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/colombianPeso>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/calorie>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/israeliNewSheqel>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/lebanesePound>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/milliwatt>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/laoKip>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/cubicInch>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/gigawattHour>", new StringParser());
	    typesToParsersBuilder.put("<http://dbpedia.org/datatype/botswanaPula>", new StringParser());


	    typesToParsers = typesToParsersBuilder.build();
	  }
  
  
}
