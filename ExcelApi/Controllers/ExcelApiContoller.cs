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
    private readonly RabbitMQConsumer _consumer;
    private readonly MySqlConnection _connection;

    public ExcelApiController(IConfiguration configuration, RabbitMQProducer publisher, RabbitMQConsumer consumer)
    {
        _publisher = publisher;
        _consumer = consumer;
        _configuration = configuration;
        _connection = new MySqlConnection(_configuration.GetConnectionString("Default"));
    }

    // Model class to handle range input for pagination
    public class Range
    {
        public int Limit { get; set; }
        public int Offset { get; set; }
    }
    public class UpdateRequest
    {
        public string? Value { get; set; }
        public int Id { get; set; }
        public string? Column { get; set; }
    }
    public class DeleteRequest
    {
        public string? Id { get; set; }
    }
    public class FindReplaceRequest
    {
        public string? FindText { get; set; }
        public string? ReplaceText { get; set; }
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
                    id = reader.GetInt32("id"),
                    email_id = reader.GetString("email_id"),
                    name = reader.GetString("name"),
                    country = reader.GetString("country"),
                    state = reader.GetString("state"),
                    telephone_number = reader.GetString("telephone_number"),
                    address_line_1 = reader.GetString("address_line_1"),
                    address_line_2 = reader.GetString("address_line_2"),
                    date_of_birth = reader.GetString("date_of_birth"),
                    gross_salary_FY2019_20 = reader.GetString("gross_salary_FY2019_20"),
                    gross_salary_FY2020_21 = reader.GetString("gross_salary_FY2020_21"),
                    gross_salary_FY2021_22 = reader.GetString("gross_salary_FY2021_22"),
                    gross_salary_FY2022_23 = reader.GetString("gross_salary_FY2022_23"),
                    gross_salary_FY2023_24 = reader.GetString("gross_salary_FY2023_24"),

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

            _consumer.ResetChunkCounter(csvData.Chunk(10000).Count());

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
    private void ConvertToQueryAsync(string[][] csvData)
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

    [HttpPost("updateRecord")]
    public async Task<IActionResult> UpdateRecord([FromBody] UpdateRequest request)
    {
        // Validate the incoming record
        if (request == null)
        {
            return BadRequest("Invalid record data.");
        }

        // Build the SQL update statement dynamically with parameterized queries
        var query = $"UPDATE FILE SET {request.Column} = @Value WHERE id = @Index;";

        // Execute the update query within a try-catch block for error handling
        try
        {
            await _connection.OpenAsync();
            await using var command = new MySqlCommand(query.ToString(), _connection);

            // Add parameters to prevent SQL injection
            command.Parameters.AddWithValue("@Value", request.Value);
            command.Parameters.AddWithValue("@Index", request.Id);


            // Execute the query and get the number of affected rows
            var result = await command.ExecuteNonQueryAsync();

            // Close the connection explicitly (optional due to using statement)
            await _connection.CloseAsync();

            // Check if any rows were affected, indicating a successful update
            if (result == 0)
            {
                // Log the failure and return a BadRequest response
                return BadRequest("Update failed: No records were affected.");
            }

            // Log the success and return an Ok response with the number of affected rows
            Console.WriteLine($"Update successful: {result} record(s) updated.");
            return Ok(new { Message = "Update successful", RowsAffected = result });
        }
        catch (MySqlException ex)
        {
            // Log the exception and return an InternalServerError response
            Console.WriteLine($"Database error occurred: {ex.Message}");
            return StatusCode(StatusCodes.Status500InternalServerError, "An error occurred while updating the record.");
        }
    }

    // API to find and replace text within the database records
    [HttpPost("findAndReplace")]
    public async Task<IActionResult> FindAndReplace([FromBody] FindReplaceRequest request)

    {
        if (request == null || string.IsNullOrEmpty(request.FindText) || request.ReplaceText == null)
        {
            return BadRequest("Invalid find and replace parameters.");
        }

        try
        {
            await _connection.OpenAsync();

            // Building the SQL query to update all fields containing the FindText
            var query = new StringBuilder();
            query.Append("UPDATE file SET ");
            query.Append("email_id = REPLACE(email_id, @FindText, @ReplaceText), ");
            query.Append("name = REPLACE(name, @FindText, @ReplaceText), ");
            query.Append("country = REPLACE(country, @FindText, @ReplaceText), ");
            query.Append("state = REPLACE(state, @FindText, @ReplaceText), ");
            query.Append("telephone_number = REPLACE(telephone_number, @FindText, @ReplaceText), ");
            query.Append("address_line_1 = REPLACE(address_line_1, @FindText, @ReplaceText), ");
            query.Append("address_line_2 = REPLACE(address_line_2, @FindText, @ReplaceText), ");
            query.Append("date_of_birth = REPLACE(date_of_birth, @FindText, @ReplaceText), ");
            query.Append("gross_salary_FY2019_20 = REPLACE(gross_salary_FY2019_20, @FindText, @ReplaceText), ");
            query.Append("gross_salary_FY2020_21 = REPLACE(gross_salary_FY2020_21, @FindText, @ReplaceText), ");
            query.Append("gross_salary_FY2021_22 = REPLACE(gross_salary_FY2021_22, @FindText, @ReplaceText), ");
            query.Append("gross_salary_FY2022_23 = REPLACE(gross_salary_FY2022_23, @FindText, @ReplaceText), ");
            query.Append("gross_salary_FY2023_24 = REPLACE(gross_salary_FY2023_24, @FindText, @ReplaceText);");

            // Prepare and execute the SQL command
            using var command = new MySqlCommand(query.ToString(), _connection);
            command.Parameters.AddWithValue("@FindText", request.FindText);
            command.Parameters.AddWithValue("@ReplaceText", request.ReplaceText);

            var result = await command.ExecuteNonQueryAsync();

            // Close the connection explicitly (optional due to using statement)
            await _connection.CloseAsync();

            // Return the number of affected rows
            return Ok(new { Message = "Find and Replace successful", RowsAffected = result });
        }
        catch (Exception ex)
        {
            // Return a 500 Internal Server Error with the exception message
            return StatusCode(500, $"Internal server error: {ex.Message}");
        }
    }

    [HttpDelete("deleteRow")]
    public async Task<IActionResult> DeleteRow([FromBody] DeleteRequest request)
    {
        if (request == null || string.IsNullOrEmpty(request.Id))
        {
            return BadRequest("Invalid request. The id field is required.");
        }

        try {

            await _connection.OpenAsync();

            using var command = _connection.CreateCommand();
            command.CommandText = "DELETE FROM file WHERE id = @id";
            command.Parameters.AddWithValue("@id", request.Id);

            var result = await command.ExecuteNonQueryAsync();

            return Ok();
        } catch (Exception ex)
        {
            // Return a 500 Internal Server Error with the exception message
            return StatusCode(500, $"Internal server error: {ex.Message}");
        }
    }
}
