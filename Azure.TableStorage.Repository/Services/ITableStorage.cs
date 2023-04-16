using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.TableStorage.Repository.Services
{
    public interface ITableStorage
    {
        Task<IEnumerable<T>> GetAllAsync<T>(string tableName) where T : class, ITableEntity, new();
        Task<IEnumerable<T>> QueryAsync<T>(string tableName, TableQuery<T> query) where T : class, ITableEntity, new();
        Task<T> GetAsync<T>(string tableName, string partitionKey, string rowKey) where T : class, ITableEntity;
        Task<object> AddOrUpdateAsync(string tableName, ITableEntity entity);
        Task<object> DeleteAsync(string tableName, ITableEntity entity);
        Task<object> AddAsync(string tableName, ITableEntity entity);
        Task<IEnumerable<T>> AddBatchAsync<T>(string tableName, IEnumerable<ITableEntity> entities, BatchOperationOptions options) where T : class, ITableEntity, new();
        Task<object> UpdateAsync(string tableName, ITableEntity entity);
        Task<CloudTable> CrateTable(string tableName);
    }
}
