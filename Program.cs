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
    var context = new DynamoDBContext(
        client, new DynamoDBContextConfig { 
            Conversion = DynamoDBEntryConversion.V2 
    });
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
    Console.WriteLine("Serialization complete");
    meteoriteBatch.AddPutItems(meteorites);

    Console.WriteLine("Performing batch write...this will take a while");
    await meteoriteBatch.ExecuteAsync();
    Console.WriteLine("Batch write executed successfully");
}

static AWSCredentials GetCredentials(string profileName)
{
    var chain = new CredentialProfileStoreChain();
    chain.TryGetAWSCredentials(profileName, out var awsCredentials);
    return awsCredentials;  
}

static List<Meteorite> GetData()
{
    Console.WriteLine("Serializing data from file");
    var fileName = "Meteorites.json";
    var jsonString = File.ReadAllText(fileName);
    var options = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };
    return JsonSerializer.Deserialize<List<Meteorite>>(jsonString, options);
}
