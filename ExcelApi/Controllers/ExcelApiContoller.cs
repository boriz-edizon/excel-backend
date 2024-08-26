using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using ExcelApi.Models;
using Microsoft.AspNetCore.Http.HttpResults;
using System.Text;
using System.Text.Json;
using ExcelApi.Services;
using RabbitMQ.Client;

namespace ExcelApi.Controllers;

[ApiController]
[Route("controller")]
public class ExcelApiController : ControllerBase
{   
    private readonly IConfiguration _confirguration;
    private MySqlConnection _connection;
    private readonly RabbitMQProducer _publisher;

    public ExcelApiController(IConfiguration configuration,  RabbitMQProducer publisher)
    {   
        _publisher = publisher;
        _confirguration = configuration;
        _connection = new MySqlConnection(_confirguration.GetConnectionString("Default"));
    } 

    public class Range
    {
        public int limit { get; set; }
        public int offset { get; set; }
    }

    [HttpPost]
    [Route("getCsv")]
    public async Task<IActionResult> GetCsv([FromBody]  Range range)
    {
        List<Excel> file = new List<Excel>(); // Assuming the data type is TodoItem; adjust as necessary

        await _connection.OpenAsync();

        Console.WriteLine("hello");

        using var command = new MySqlCommand($"SELECT * FROM file limit {range.limit} offset {range.offset};", _connection);
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var item = new Excel
            {
                email_id = reader.GetString("email_id"),
                name = reader.GetString("name"),
                country = reader.GetString("country"),
                state = reader.GetString("state"),
                telephone_number = long.Parse(reader.GetString("telephone_number")),
                address_line_1 = reader.GetString("address_line_1"),
                address_line_2 = reader.GetString("address_line_2"),
                date_of_birth = reader.GetString("date_of_birth"),
                gross_salary_FY2019_20 = int.Parse(reader.GetString("gross_salary_FY2019_20")),
                gross_salary_FY2020_21 = int.Parse(reader.GetString("gross_salary_FY2020_21")),
                gross_salary_FY2021_22 = int.Parse(reader.GetString("gross_salary_FY2021_22")),
                gross_salary_FY2022_23 = int.Parse(reader.GetString("gross_salary_FY2022_23")),
                gross_salary_FY2023_24 = int.Parse(reader.GetString("gross_salary_FY2023_24")),

            };
            file.Add(item);
        }


        return Ok(file);
    }

    [HttpPost]
    [Route("uploadCsv")]
    public async Task<IActionResult> OnPostUploadAsync(IFormFile csvFile)
    {            


        if (csvFile == null || csvFile.Length == 0)
        {
            return BadRequest("Please upload a valid CSV file.");
        }

        var csvData = new List<string[]>();

        // Console.WriteLine("start time "+((DateTimeOffset)DateTime.UtcNow).ToUnixTimeMilliseconds());


        var reader = new StreamReader(csvFile.OpenReadStream());

        await reader.ReadLineAsync();

        while (await reader.ReadLineAsync() is string line){
            var values = line.Split(',');  // return  an array whose elements contain the substrings with delimited ","
            csvData.Add(values);
        }


        foreach (var chunk in csvData.Chunk(10000)){
            CovertToQuery(chunk);
        }


        return Ok("CSV file uploaded");
    }

    private  void CovertToQuery(string[][] csvData)
    {
        var watch = new System.Diagnostics.Stopwatch();
        watch.Start();
        
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

        _publisher.produce(query.ToString());
        watch.Stop();
        // Console.WriteLine($"Controller Execution Time: {watch.ElapsedMilliseconds} ms");
    }
}