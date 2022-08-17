using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;

// using the shared credential store locally;
var client = new AmazonDynamoDBClient(
    GetCredentials("dynamoDBCredential"), RegionEndpoint.USWest2);

try
{
    var context = new DynamoDBContext(client);
    await BatchWrite(context);
}
catch (AmazonServiceException e) { Console.WriteLine(e.Message); }
catch (Exception e) { Console.WriteLine(e.Message); }

Console.WriteLine("To continue, press Enter");
Console.ReadLine();

static async Task BatchWrite(DynamoDBContext context)
{
    
    var meteoriteBatch = context.CreateBatchWrite<Meteorite>();
    var meteorites = GetData();
    meteoriteBatch.AddPutItems(meteorites);

    Console.WriteLine("Performing batch write in SingleTableBatchWrite().");
    await meteoriteBatch.ExecuteAsync();
    Console.WriteLine("Batch write executed successfully");
}

static AWSCredentials GetCredentials(string profileName)
{
    var chain = new CredentialProfileStoreChain();
    chain.TryGetAWSCredentials(profileName, out var awsCredentials);
    return awsCredentials;  
}

static HashSet<Meteorite> GetData()
{
    var fileName = "Meteorites.json";
    var jsonString = File.ReadAllText(fileName);
    var options = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };
    var meteorites = System.Text.Json.JsonSerializer.Deserialize<List<Meteorite>>(jsonString, options);

    // checking for duplication in the collection -> there shouldn't be any
    IEnumerable<string> duplicates = meteorites.GroupBy(x => x.Id)
                                        .Where(g => g.Count() > 1)
                                        .Select(x => x.Key);

    if (duplicates.Any())
    {
        Console.WriteLine("duplicates found");
    }

    // second chedk for duplication -> doubly sure there are no duplicate items
    var uniqueList = new HashSet<Meteorite>();
    foreach(Meteorite m in meteorites)
    {
        var result = uniqueList.Add(m);
        if (result == false)
        {
            throw new Exception("duplicate item found");
        }
    }
    return uniqueList;

}
