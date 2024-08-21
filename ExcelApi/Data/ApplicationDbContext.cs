using Microsoft.EntityFrameworkCore;
using ExcelApi.Models;

namespace ExcelApi.Data;

public class ApplicationDbContext : DbContext
{
    public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : base (options)
    {

    }
    public DbSet<Excel> file { get; set; } = null!;
}