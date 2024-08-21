using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using ExcelApi.Models;
using Microsoft.AspNetCore.Http.HttpResults;
using System.Text;

namespace ExcelApi.Controllers;

[ApiController]
[Route("controller")]
public class ExcelApiController : ControllerBase
{   
    private readonly IConfiguration _confirguration;
    private MySqlConnection connection;
    public ExcelApiController(IConfiguration configuration)
    {
        _confirguration = configuration;
        connection = new MySqlConnection(_confirguration.GetConnectionString("Default"));
    } 

    // [HttpGet]
    // public async Task<IActionResult> GetExcelItem()
    // {
    //     var file = new List<Excel>(); // Assuming the data type is TodoItem; adjust as necessary

        // await connection.OpenAsync();

        // using var command = new MySqlCommand("SELECT * FROM persons;", connection);
        // using var reader = await command.ExecuteReaderAsync();
        // while (await reader.ReadAsync())
        // {
        //     var item = new Excel
        //     {
        //         Id = reader.GetInt32("PersonId"),
        //     };
        //     file.Add(item);
        // }
    //     return Ok(file);
    // }

    [HttpPost]
    [Route("uploadCsv")]
    public async Task<IActionResult> OnPostUploadAsync(IFormFile csvFile)
    {            


        if (csvFile == null || csvFile.Length == 0)
        {
            return BadRequest("Please upload a valid CSV file.");
        }

        var csvData = new List<string[]>();

        var reader = new StreamReader(csvFile.OpenReadStream());

        await reader.ReadLineAsync();

        while (await reader.ReadLineAsync() is string line){
            var values = line.Split(',');  // return  an array whose elements contain the substrings with delimited ","
            csvData.Add(values);
        }


        foreach (var chunk in csvData.Chunk(10000)){
            await InsertDataIntoDatabase(chunk);
        }


        return Ok("CSV file uploaded");
    }

    private async Task InsertDataIntoDatabase(string[][] csvData)
    {
        var watch = new System.Diagnostics.Stopwatch();
        watch.Start();
        
        await connection.OpenAsync();
        var query = new StringBuilder();
        query.Append("INSERT INTO file (email_id, name, country, state, city, telephone_number, address_line_1, address_line_2, date_of_birth, gross_salary_FY2019_20, gross_salary_FY2020_21, gross_salary_FY2021_22, gross_salary_FY2022_23, gross_salary_FY2023_24) VALUES ");
        foreach (var row in csvData)
        {
            // query.Append($"('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5]}', '{row[6]}', '{row[7]}', '{row[8]}', '{row[9]}', '{row[10]}', '{row[11]}', '{row[12]}', '{row[13]}'),");
            query.Append($"('{row[0]}', " + 
                $"'{row[1]}', " +
                $"'{row[2]}', " +
                $"'{row[3]}', " +
                $"'{row[4]}', " +
                $"'{row[5]}', " +
                $"'{row[6]}', " +
                $"'{row[7]}', " +
                $"'{row[8]}', " +
                $"'{row[9]}', " +
                $"'{row[10]}', " +
                $"'{row[11]}', " +
                $"'{row[12]}', " +
                $"'{row[13]}') ,"
            );
        }
        query.Length--;
        query.Append(';');
        var command = new MySqlCommand(query.ToString(), connection);
        await command.ExecuteNonQueryAsync();
        await connection.CloseAsync();
        watch.Stop();
        Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
    }
}