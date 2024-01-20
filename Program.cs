
using Microsoft.Data.SqlClient;
using Dapper;
using Microsoft.Identity.Client;
using Microsoft.Rest;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.Azure.Management.DataFactory;
using Newtonsoft.Json;
using Microsoft.Rest.Serialization;



using System;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Generic;
using System.Net;
using Microsoft.Azure.Cosmos;
using System.Xml;
using Newtonsoft.Json.Linq;
using System.Data;





string tenantID = "xxxxx-xxxxx-xxxx-xxxx-xxxxx";
string applicationId = "xxxxx-xxxxx-xxxx-xxxxx-xxxxx";
string authenticationKey = "Xxxxx~xxxxx-xxxxxxxx";
string subscriptionId = "xxxxx-xxxx-xxxx-xxxx-xxxxx";
string resourceGroup = "xxxxx-xx-xxx";


string region = "East US";
//string dataFactoryName =
//   "BlertbInputDataFactory";
//string IRName = "BillsPastryShopIntegratedRuntime";

//string fsDatasetName = "FileServerDataset";
//string DPMSDatasetFolder1 = "DPMS1";
//string MyDatabaseName = "DPMS1";

//string json = @"{ ""dpms_source"": ""DPMS1"", ""id"": ""$.pastry_id""} ";
//string additionalcolumns = @"[{""name"":""dpms_source"",""value"":{""value"":""DPMS1"",""type"":""Expression""}}]";
//json.Replace(@"\", " ");
//additionalcolumns.Replace(@"\", " ");

IConfidentialClientApplication app = ConfidentialClientApplicationBuilder.Create(applicationId)
 .WithAuthority("https://login.microsoftonline.com/" + tenantID)
 .WithClientSecret(authenticationKey)
 .WithLegacyCacheCompatibility(false)
 .WithCacheOptions(CacheOptions.EnableSharedCacheOptions)
 .Build();

AuthenticationResult result = await app.AcquireTokenForClient(
  new string[] { "https://management.azure.com//.default" })
   .ExecuteAsync();
ServiceClientCredentials cred = new TokenCredentials(result.AccessToken);
var client = new DataFactoryManagementClient(cred)
{
    SubscriptionId = subscriptionId
};




var builder = new SqlConnectionStringBuilder();
builder.DataSource = "xxx-sql.database.windows.net";
builder.UserID = "xxxxxx";
builder.Password = "xxxxx";
builder.InitialCatalog = "Xxxxxx";

using SqlConnection connection = new SqlConnection(builder.ConnectionString);
SqlCommand cmd = new SqlCommand("SELECT * from tblClientConfigurations4 ", connection);


SqlDataAdapter da = new SqlDataAdapter(cmd);
DataTable dt = new DataTable();
da.Fill(dt);


IDictionary<string, object> properties = new Dictionary<string, object>();
IDictionary<string, ParameterSpecification> properties2 = new Dictionary<string, ParameterSpecification>();
IList<object> annotations = new List<object>();

foreach (DataRow dr in dt.Rows)
{


    string client_id = dr["client_id"].ToString();
    string client_name = dr["client_name"].ToString();
    string data_factory_name = dr["data_factory_name"].ToString();
    string integration_runtime_name = dr["integration_runtime_name"].ToString();
    string ir_authkey_1 = dr["ir_auth_key1"].ToString();
    string ir_authkey_2 = dr["ir_auth_key2"].ToString();
    string linked_service = dr["LinkedService"].ToString();
    string data_set_name = dr["dataset_name"].ToString();
    string DPMSInstance = dr["DPMSInstance"].ToString();
    string filename = dr["File"].ToString();
    string container_id = dr["ContainerID"].ToString();
    string pipelineName = "POC_Pipeline";


    // var adfFactory = await client.Factories.GetAsync(resourceGroup, data_factory_name);

    //  var integrationRuntime = await client.IntegrationRuntimes.GetAsync(resourceGroup, data_factory_name, integration_runtime_name);




    //LinkedServiceReference fileLinkedServiceReference = new LinkedServiceReference(linked_service);




    Console.WriteLine("Creating data factory " + data_factory_name + "...");
    Factory dataFactory = new Factory
    {
        Location = region,
        Identity = new FactoryIdentity()
    };
    client.Factories.CreateOrUpdate(resourceGroup, data_factory_name, dataFactory);
    Console.WriteLine(
        SafeJsonConvert.SerializeObject(dataFactory, client.SerializationSettings));

    while (client.Factories.Get(resourceGroup, data_factory_name).ProvisioningState ==
           "PendingCreation")
    {
        System.Threading.Thread.Sleep(1000);
    }






    var adfFactory = await client.Factories.GetAsync(resourceGroup, data_factory_name);




    var res = await client.IntegrationRuntimes.CreateOrUpdateWithHttpMessagesAsync(resourceGroup, data_factory_name,
          integration_runtime_name,
           new IntegrationRuntimeResource()
           { Properties = new SelfHostedIntegrationRuntime() }

              //LinkedInfo = new LinkedIntegrationRuntimeRbacAuthorization()
              //{
              //    // ResourceId = ""
              //}
              );

    var integrationRuntime = await client.IntegrationRuntimes.GetAsync(resourceGroup, data_factory_name, integration_runtime_name);





    LinkedServiceResource FileLinkedService = new LinkedServiceResource(
 new FileServerLinkedService(
     "C:\\Users\\dentaladmin\\Documents\\JSON",
     properties, new IntegrationRuntimeReference(integration_runtime_name),
     "NewFileLinkedService",
     properties2,
     annotations,
     "dentaladmin",
     new SecureString("C@restream1!"),
     "IR@baee399c-a45c-4b5d-89cf-313fcb8d32c4@csd-datafactory@ServiceEndpoint=csd-datafactory.eastus.datafactory.azure.net@5UHRkyLOIZv18aj717FSDR7Jp6l1Qqf07XSAGxmimEA="),
      "LSR",
      "SLR",
      "FileServer",
      "Test");

    Console.WriteLine(SafeJsonConvert.SerializeObject(FileLinkedService, client.SerializationSettings));
    var fsLinkedService = client.LinkedServices.CreateOrUpdate(resourceGroup, data_factory_name, "NewFileLinkedService", FileLinkedService);
    LinkedServiceReference fileLinkedServiceReference = new LinkedServiceReference("NewFileLinkedService");




    LinkedServiceReference cosmosLinkedServiceReference = new LinkedServiceReference(DPMSInstance);

    //Dataset file_ds = new Dataset(fileLinkedServiceReference);
    //DatasetResource fileDSResource = new DatasetResource(file_ds);
    //var file_dataset = await client.Datasets.CreateOrUpdateWithHttpMessagesAsync(resourceGroup, data_factory_name, data_set_name, fileDSResource);



    // FolderPath = new Microsoft.Azure.Management.DataFactory.Models.Expression { Value = "@{dataset().path}" },
    //    Parameters = new Dictionary<string, ParameterSpecification>



    Console.WriteLine("Creating dataset " + data_set_name + "...");
    DatasetResource fileDataset = new DatasetResource(

         new FileShareDataset
         {
             LinkedServiceName = new LinkedServiceReference
             {
                 ReferenceName = "NewFileLinkedService"
             },
             FileName = filename + ".json",
             Format = new JsonFormat()
             {
                 //  JsonPathDefinition = JsonConvert.SerializeObject(new1)
             }
         }



    );



    client.Datasets.CreateOrUpdate(resourceGroup, data_factory_name, "NewFileDataset", fileDataset);
    Console.WriteLine(
        SafeJsonConvert.SerializeObject(fileDataset, client.SerializationSettings));

    DatasetResource cosmosDbDataset = new DatasetResource(
    new DocumentDbCollectionDataset
    {
        LinkedServiceName = new LinkedServiceReference
        {
            ReferenceName = DPMSInstance
        },
        CollectionName = filename
    }
);
    client.Datasets.CreateOrUpdate(resourceGroup, data_factory_name, DPMSInstance, cosmosDbDataset);
    Console.WriteLine(SafeJsonConvert.SerializeObject(cosmosDbDataset, client.SerializationSettings));









    Console.WriteLine("Creating pipeline " + pipelineName + "...");



    PipelineResource pipeline = new PipelineResource
    {
        Activities = new List<Activity>
            {
                  new  CopyActivity
                {
                    Name = "CopyFromFileServiceToCosmosDB",

                    Inputs = new List<DatasetReference>
                    {
                        new DatasetReference()
                        {
                            ReferenceName = "NewFileDataset"
                        }
                    },
                    Outputs = new List<DatasetReference>
                    {
                        new DatasetReference
                        {
                            ReferenceName = DPMSInstance
                        }
                    },

                    Source = new JsonSource { },
                    Sink = new CosmosDbSqlApiSink{ }
            }
        }
    };
    client.Pipelines.CreateOrUpdate(resourceGroup, data_factory_name, pipelineName, pipeline);
    Console.WriteLine(SafeJsonConvert.SerializeObject(pipeline, client.SerializationSettings));

    Console.WriteLine("Creating Pipeline run...");
    CreateRunResponse runResponse = client.Pipelines.CreateRunWithHttpMessagesAsync(resourceGroup, data_factory_name, pipelineName).Result.Body;
    Console.WriteLine("Pipeline run ID: " + runResponse.RunId);


}