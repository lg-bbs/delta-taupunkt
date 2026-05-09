using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace TauLuftAspire.ApiService.Migrations
{
    /// <inheritdoc />
    public partial class Config : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Config",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    Interval = table.Column<int>(type: "integer", nullable: false),
                    TargetDelta = table.Column<decimal>(type: "numeric", nullable: false),
                    Hysrerese = table.Column<decimal>(type: "numeric", nullable: false),
                    MinTempInside = table.Column<decimal>(type: "numeric", nullable: false),
                    MinTempOutside = table.Column<decimal>(type: "numeric", nullable: false),
                    TempInsideOffset = table.Column<decimal>(type: "numeric", nullable: false),
                    HumInsideOffset = table.Column<decimal>(type: "numeric", nullable: false),
                    TempOutsideOffset = table.Column<decimal>(type: "numeric", nullable: false),
                    HumOutsideOffset = table.Column<decimal>(type: "numeric", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Config", x => x.Id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Config");
        }
    }
}
