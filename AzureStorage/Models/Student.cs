using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorage.Models
{
    public class Student:TableEntity
    {
        public int ID { get; set; }
        public string name { get; set; }
        public string address { get; set; }
        public string department { get; set; }

        public void AssignRowKey()
        {
            this.RowKey = ID.ToString();
        }
        public void AssignPartitionKey()
        {
            this.PartitionKey = department;
        }
    }

    public class StudentObj 
    {
        public int ID { get; set; }
        public string name { get; set; }
        public string address { get; set; }
        public string department { get; set; }
    }
}
