using Azure.TableStorage.Repository.Services;
using AzureStorage.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Concurrent;

namespace AzureStorage.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class GenericController : ControllerBase
    {
        private readonly ITableStorage _tableService;

        public GenericController(ITableStorage tableService)
        {
            _tableService = tableService;
        }

        [AcceptVerbs("POST")]
        [Route("CreateTable")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<IActionResult> CreateTable(string tableName)
        {
            try
            {
                var tablresponse = await _tableService.CrateTable(tableName);
                return Ok(tablresponse);
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
                var student = new Student();

                student.ID = studentobj.ID;
                student.name = studentobj.name;
                student.address = studentobj.address;
                student.department = studentobj.department;
                student.AssignPartitionKey();
                student.AssignRowKey();

                dynamic stu = await _tableService.AddOrUpdateAsync(tableName, student);
                return Ok("Record inserted");
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        #region private method

        #endregion
    }
}
