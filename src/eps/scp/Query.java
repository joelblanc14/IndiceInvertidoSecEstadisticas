package eps.scp;

/**
 * Created by Nando on 8/10/19.
 */
public class Query
{

    public static void main(String[] args)
    {
        InvertedIndex hash;
        String queryString=null, indexDirectory=null;

        if (args.length !=2)
            System.err.println("Error in Parameters. Usage: Query <String> <IndexDirectory>");
        if (args.length > 0)
            queryString = args[0];
        if (args.length > 1)
            indexDirectory = args[1];

        hash = new InvertedIndex();
        hash.loadIndex(indexDirectory);
        //hash.PrintIndex();
        hash.query(queryString);
    }

}
