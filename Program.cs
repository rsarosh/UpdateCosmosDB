using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Sql;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;
using System.Threading;
using System.Configuration;
using System.Diagnostics;

namespace SQL
{
    class Program
    {
        static DocumentClient client;
        static void Main(string[] args)
        {

            string endpoint = ConfigurationManager.AppSettings["endpoint"];
            string primaryKey = ConfigurationManager.AppSettings["primaryKey"];
            string connectionString = ConfigurationManager.AppSettings["connectionString"];
            string database = ConfigurationManager.AppSettings["database"]; 
            string collection = ConfigurationManager.AppSettings["collection"];

            client = new DocumentClient(new Uri(endpoint), primaryKey);
            
            SqlConnection cn = new SqlConnection(connectionString);
            try
            {
                string cmdText = File.ReadAllText(AppDomain.CurrentDomain.BaseDirectory + @"\cmd.txt");
                cn.Open();
                SqlCommand cmd = new SqlCommand(cmdText, cn);
                var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                ProcessReader(reader, database, collection);
            } catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                cn.Close();
            }

            Console.Read();
            return;
        }

        private static async void ProcessReader(SqlDataReader reader, string databaseid, string collectionid)
        {
            int ctr = 0;
            int collession = 0;
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            while (reader.Read())
            {
                    Console.WriteLine(ctr++); //which row is getting processed
                    //cleanup tags
                    string[] _tags = reader["tags"].ToString().Replace('[', ' ').Replace(']', ' ').Split(new char[] { ',' });
                    for (int i = 0; i < _tags.Count(); i++)
                    {
                        _tags[i] = _tags[i].Replace("\"", "").Trim();
                    }
                    //cleanup authors
                    string[] _authors = reader["authors"].ToString().Replace('[', ' ').Replace(']', ' ').Split(new char[] { ',' });
                    for (int i = 0; i < _authors.Count(); i++)
                    {
                        _authors[i] = _authors[i].Replace("\"", "").Trim();
                    }
                    video v = new video
                    {
                        Id = reader["id"].ToString(),
                        url = reader["url"].ToString(),
                        title = reader["title"].ToString(),
                        tags = _tags,
                        authors = _authors,
                        primaryAudience = reader["primaryAudience"].ToString(),
                        totalViewCount = Convert.ToInt32(reader["totalViewCount"]),
                        ratingCount = Convert.ToInt16(reader["ratingCount"]),
                        rating = Convert.ToDouble(reader["rating"]),
                        type = reader["type"].ToString(),
                        previewImage = reader["previewImage"].ToString(),

                    };
                try
                {
                    await UpdateDocumentDB(v, databaseid, collectionid);
                } catch (Exception ex)
                {
                    if (ex.HResult == -2146233088)
                    {
                        //document already existing
                        Console.Write("*");
                        collession++;
                    }
                }
            }
            stopWatch.Stop();
            Console.WriteLine("{0} records processed. {1} collesssion happened.  in {2} hr {3} Min {4} Sec", ctr, collession, stopWatch.Elapsed.Hours, stopWatch.Elapsed.Minutes, stopWatch.Elapsed.Seconds);
        }
        private static async Task<ResourceResponse<Document>> UpdateDocumentDB(video v, string databaseid, string collectionid)
        {
            DocumentCollection myCollection = new DocumentCollection();
            myCollection.Id = collectionid;
            myCollection.PartitionKey.Paths.Add("/id");
            
            ////create collection in the UI

            //await client.CreateDocumentCollectionAsync(
            //    UriFactory.CreateDatabaseUri(databaseid),
            //    myCollection,
            //    new RequestOptions { OfferThroughput = 2500 });
            return await client.CreateDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(databaseid, collectionid),
                        v);
            }
            
        }

    public class video
    {
        [JsonProperty("id")]
        public string Id;

        [JsonProperty("url")]
        public string url;

        //[JsonConverter(typeof(IsoDateTimeConverter))]
        //[JsonProperty("title")]
        //public DateTime title;

        [JsonProperty("title")]
        public string title;

        [JsonProperty("tags")]
        public string [] tags;

        [JsonProperty("previewImage")]
        public string previewImage;

        [JsonProperty("type")]
        public string type;

        [JsonProperty("totalviewCount")]
        public int totalviewCount;

        [JsonProperty("rating")]
        public double rating;

        [JsonProperty("ratingCount")]
        public int ratingCount;

        [JsonProperty("totalViewCount")]
        public int totalViewCount;

        [JsonProperty("authors")]
        public string [] authors;

        [JsonProperty("primaryAudience")]
        public string primaryAudience;


    }
}
