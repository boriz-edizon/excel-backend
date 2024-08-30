using Microsoft.AspNetCore.Mvc;
using MySqlConnector;
using ExcelApi.Models;
using System.Text;
using ExcelApi.Services;

namespace ExcelApi.Controllers;

[ApiController]
[Route("[controller]")]
public class ExcelApiController : ControllerBase
{
    private readonly IConfiguration _configuration;
    private readonly RabbitMQProducer _publisher;
    private readonly MySqlConnection _connection;

    public ExcelApiController(IConfiguration configuration, RabbitMQProducer publisher)
    {
        _publisher = publisher;
        _configuration = configuration;
        _connection = new MySqlConnection(_configuration.GetConnectionString("Default"));
    }

    // Model class to handle range input for pagination
    public class Range
    {
        public int Limit { get; set; }
        public int Offset { get; set; }
    }

    // API to fetch a range of CSV records from the database
    [HttpPost("getCsv")]
    public async Task<IActionResult> GetCsv([FromBody] Range range)
    {
        if (range == null || range.Limit <= 0 || range.Offset < 0)
        {
            return BadRequest("Invalid range parameters.");
        }

        List<Excel> file = new List<Excel>();

        try
        {
            await _connection.OpenAsync();

            // Parameterized query to prevent SQL injection
            string query = $"SELECT * FROM file LIMIT @Limit OFFSET @Offset;";
            using var command = new MySqlCommand(query, _connection);
            command.Parameters.AddWithValue("@Limit", range.Limit);
            command.Parameters.AddWithValue("@Offset", range.Offset);

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
        }
        catch (Exception ex)
        {
            // Return a 500 Internal Server Error with the exception message
            return StatusCode(500, $"Internal server error: {ex.Message}");
        }
        finally
        {
            // Ensure the connection is closed
            await _connection.CloseAsync();
        }

        return Ok(file);
    }

    // API to upload a CSV file and process it in chunks
    [HttpPost("uploadCsv")]
    public async Task<IActionResult> UploadCsv(IFormFile csvFile)
    {
        if (csvFile == null || csvFile.Length == 0)
        {
            return BadRequest("Please upload a valid CSV file.");
        }

        var csvData = new List<string[]>();

        try
        {
            // Read the CSV file line by line
            using var reader = new StreamReader(csvFile.OpenReadStream());
            while (await reader.ReadLineAsync() is string line)
            {
                var values = line.Split(',');
                csvData.Add(values);
            }

            // Process the CSV data in chunks to avoid 
            foreach (var chunk in csvData.Chunk(10000))
            {
                 ConvertToQueryAsync(chunk);
            }
        }
        catch (Exception ex)
        {
            // Return a 500 Internal Server Error with the exception message
            return StatusCode(500, $"Error processing CSV file: {ex.Message}");
        }

        return Ok("CSV file uploaded successfully.");
    }

    // Convert CSV data into a SQL INSERT query and send it to RabbitMQ
    private void  ConvertToQueryAsync(string[][] csvData)
    {
        var query = new StringBuilder();
        query.Append("INSERT INTO file (email_id, name, country, state, city, telephone_number, address_line_1, address_line_2, date_of_birth, gross_salary_FY2019_20, gross_salary_FY2020_21, gross_salary_FY2021_22, gross_salary_FY2022_23, gross_salary_FY2023_24) VALUES ");

        foreach (var row in csvData)
        {
            // Construct the query for each row of data
            query.Append($"('{MySqlHelper.EscapeString(row[0])}', " +
                         $"'{MySqlHelper.EscapeString(row[1])}', " +
                         $"'{MySqlHelper.EscapeString(row[2])}', " +
                         $"'{MySqlHelper.EscapeString(row[3])}', " +
                         $"'{MySqlHelper.EscapeString(row[4])}', " +
                         $"'{MySqlHelper.EscapeString(row[5])}', " +
                         $"'{MySqlHelper.EscapeString(row[6])}', " +
                         $"'{MySqlHelper.EscapeString(row[7])}', " +
                         $"'{MySqlHelper.EscapeString(row[8])}', " +
                         $"'{MySqlHelper.EscapeString(row[9])}', " +
                         $"'{MySqlHelper.EscapeString(row[10])}', " +
                         $"'{MySqlHelper.EscapeString(row[11])}', " +
                         $"'{MySqlHelper.EscapeString(row[12])}', " +
                         $"'{MySqlHelper.EscapeString(row[13])}'),");
        }

        // Remove the last comma and add a semicolon to complete the query
        query.Length--;
        query.Append(';');

        try
        {
            // Publish the query to RabbitMQ
            _publisher.produce(query.ToString());
        }
        catch (Exception ex)
        {
            // Log the error or handle it accordingly
            Console.WriteLine($"Failed to produce message: {ex.Message}");
        }
    }
}
