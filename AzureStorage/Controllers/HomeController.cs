using AzureStorage.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Principal;

namespace AzureStorage.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HomeController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly CloudStorageAccount _cloudStorageAccount;
        private readonly CloudTableClient _tableClient;
        private ConcurrentDictionary<string, CloudTable> _cloudTable;

        public HomeController(IConfiguration configuration)
        {
            _configuration = configuration;
            _cloudStorageAccount= CloudStorageAccount.Parse(_configuration["ConnectionStrings:AzureTableStorageConnectionString"]);
            _tableClient = _cloudStorageAccount.CreateCloudTableClient();
            _cloudTable = new ConcurrentDictionary<string, CloudTable>();
        }

        [AcceptVerbs("POST")]
        [Route("CreateTable")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> CreateTable(string tableName)
        {
            try
            {
                CloudTable cloudTable = _tableClient.GetTableReference(tableName);
                if (await cloudTable.CreateIfNotExistsAsync())
                {
                    return Ok("Table  : " + tableName + " Created . Please check azure portal");
                }
                else
                {
                    return Ok("Table  : " + tableName + " already exists ");
                }
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        [AcceptVerbs("GET")]
        [Route("GetAll")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> GetAll(string tableName)
        {
            try
            {
                CloudTable cloudTable = _tableClient.GetTableReference(tableName);
                var query = new TableQuery<Student>().Take(2);
                IEnumerable <Student> allstudent = await QueryAsync(cloudTable, query);
                return Ok(allstudent);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        [AcceptVerbs("POST")]
        [Route("AddNewRecord")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> AddNewRecord(StudentObj studentobj, string tableName)
        {
            try
            {
                CloudTable cloudTable = _tableClient.GetTableReference(tableName);
                var student = new Student();

                student.ID = studentobj.ID;
                student.name = studentobj.name;
                student.address = studentobj.address;
                student.department= studentobj.department;
                student.AssignPartitionKey();
                student.AssignRowKey();

                Student Entity = await RetrieveRecord(cloudTable, student.PartitionKey, student.RowKey);

                if (Entity == null)
                {
                    TableOperation tableOperation = TableOperation.Insert(student);
                    await cloudTable.ExecuteAsync(tableOperation);
                    return Ok("Record inserted");
                }
                else
                {
                    return Ok("Record not inserted");
                }
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        [AcceptVerbs("POST")]
        [Route("UpdateRecord")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> UpdateRecord(StudentObj studentobj, string tableName, string PartitionKey, string RowKey)
        {
            try
            {
                CloudTable cloudTable = _tableClient.GetTableReference(tableName);
                Student Entity = await RetrieveRecord(cloudTable, PartitionKey, RowKey);
                if (Entity != null)
                {
                    Entity.name = studentobj.name;
                    Entity.address = studentobj.address;

                    TableOperation tableOperation = TableOperation.Replace(Entity);
                    await cloudTable.ExecuteAsync(tableOperation);
                    return Ok("Record updated");
                }
                else
                {
                    return Ok("Record not inserted");
                }
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        [AcceptVerbs("POST")]
        [Route("Delete")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> Delete(string tableName, string RowKey, string PartitionKey)
        {
            try
            {
                CloudTable cloudTable = _tableClient.GetTableReference(tableName);
                Student studentEntity = await RetrieveRecord(cloudTable, PartitionKey, RowKey);
                if (studentEntity != null)
                {
                    dynamic x = await DeleteAsync(tableName, studentEntity);
                    return Ok("Record deleted");
                }
                else
                {
                    return Ok("Record does not exists");
                }
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        #region Private Method

        /// <summary>
        /// Gets entities by query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        private async Task<IEnumerable<T>> QueryAsync<T>(CloudTable table, TableQuery<T> query) where T : class, ITableEntity, new()
        {
            var entities = new List<T>();
            TableContinuationToken token = null;
            do
            {
                var queryResult = await table.ExecuteQuerySegmentedAsync(query, token).ConfigureAwait(false);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (token != null);

            return entities;
        }

        /// <summary>
        /// Get entities by query with TakeCount parameter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        private async Task<IEnumerable<T>> QueryAsyncWithTakeCount<T>(CloudTable table, TableQuery<T> query) where T : class, ITableEntity, new()
        {
            var entities = new List<T>();

            const int maxEntitiesPerQueryLimit = 1000;
            var totalTakeCount = query.TakeCount;
            var remainingRecordsToTake = query.TakeCount;

            TableContinuationToken token = null;
            do
            {
                query.TakeCount = remainingRecordsToTake >= maxEntitiesPerQueryLimit ? maxEntitiesPerQueryLimit : remainingRecordsToTake;
                remainingRecordsToTake -= query.TakeCount;

                var queryResult = await table.ExecuteQuerySegmentedAsync(query, token).ConfigureAwait(false);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (entities.Count < totalTakeCount && token != null);

            return entities;
        }
        private async Task<Student> RetrieveRecord(CloudTable table, string partitionKey, string rowKey)
        {
            TableOperation tableOperation = TableOperation.Retrieve<Student>(partitionKey, rowKey);
            TableResult tableResult = await table.ExecuteAsync(tableOperation);
            var studentobj = tableResult.Result as Student;
            return studentobj;
        }

        /// <summary>
        /// Deletes the entity.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        private async Task<object> DeleteAsync(string tableName, ITableEntity entity)
        {
            var table = await EnsureTable(tableName).ConfigureAwait(false);

            TableOperation deleteOperation = TableOperation.Delete(entity);

            TableResult result = await table.ExecuteAsync(deleteOperation).ConfigureAwait(false);

            return result.Result;
        }

        /// <summary>
        /// Ensures existence of the table.
        /// </summary>
        private async Task<CloudTable> EnsureTable(string tableName)
        {
            if (!_cloudTable.ContainsKey(tableName))
            {
                var table = _tableClient.GetTableReference(tableName);
                await table.CreateIfNotExistsAsync().ConfigureAwait(false);
                _cloudTable[tableName] = table;
            }

            return _cloudTable[tableName];
        }
        #endregion
    }
}
