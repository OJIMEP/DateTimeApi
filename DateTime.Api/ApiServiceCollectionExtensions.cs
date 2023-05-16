using AuthLibrary.Data;
using DateTimeService.Api.Filters;
using DateTimeService.Api.Middlewares;
using FluentValidation;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;
using Microsoft.Net.Http.Headers;
using Microsoft.OpenApi.Models;
using Serilog;
using System.Text;

namespace DateTimeService.Api
{
    public static class ApiServiceCollectionExtensions
    {
        public static IServiceCollection AddApi(this IServiceCollection services, IConfiguration configuration)
        {
            services
                .AddAuthorizationAuthentication(configuration)
                .AddScoped<IUserService, UserService>()
                .AddSwagger()
                .AddScoped<LogActionFilter>()
                .AddTransient<GlobalExceptionHandlingMiddleware>()
                .AddValidatorsFromAssemblyContaining<Program>(ServiceLifetime.Transient);

            return services;
        }

        public static IServiceCollection AddAuthorizationAuthentication(this IServiceCollection services, IConfiguration configuration)
        {
            // Для авторизации  
            services.AddDbContext<DateTimeServiceContext>(options => options.UseSqlServer(configuration.GetConnectionString("DateTimeServiceContextConnection")));

            // For Identity  
            services.AddDefaultIdentity<DateTimeServiceUser>()
                .AddRoles<IdentityRole>()
                .AddEntityFrameworkStores<DateTimeServiceContext>()
                .AddDefaultTokenProviders();

            services.Configure<IdentityOptions>(options =>
            {
                // Default Password settings.
                options.Password.RequireDigit = true;
                options.Password.RequireLowercase = true;
                options.Password.RequireNonAlphanumeric = false;
                options.Password.RequireUppercase = false;
                options.Password.RequiredLength = 6;
                options.Password.RequiredUniqueChars = 1;
            });

            // Adding Authentication  
            services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = "JWT_OR_COOKIE";
                options.DefaultChallengeScheme = "JWT_OR_COOKIE";
                options.DefaultScheme = "JWT_OR_COOKIE";
            })
            // Adding Jwt Bearer  
            .AddJwtBearer(options =>
            {
                options.SaveToken = true;
                options.RequireHttpsMetadata = true;
                options.TokenValidationParameters = new TokenValidationParameters()
                {
                    ValidateIssuer = true,
                    ValidateAudience = true,
                    ValidAudience = configuration["JWT:ValidAudience"],
                    ValidIssuer = configuration["JWT:ValidIssuer"],
                    IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(configuration["JWT:Secret"])),
                    ValidateLifetime = true
                };
                options.Events = new JwtBearerEvents()
                {
                    OnAuthenticationFailed = c =>
                    {
                        Log.Error(c.Exception.ToString());
                        return Task.CompletedTask;
                    }

                };
            })
            .AddPolicyScheme("JWT_OR_COOKIE", "JWT_OR_COOKIE", options =>
            {
                // runs on each request
                options.ForwardDefaultSelector = context =>
                {
                    // filter by auth type
                    string authorization = context.Request.Headers[HeaderNames.Authorization];
                    if (!string.IsNullOrEmpty(authorization) && authorization.StartsWith("Bearer "))
                        return JwtBearerDefaults.AuthenticationScheme;

                    // otherwise always check for Identity cookie auth
                    return IdentityConstants.ApplicationScheme;
                };
            });

            services.AddAuthorization(builder =>
            {
                builder.AddPolicy("Hangfire", policy => policy.RequireRole(UserRoles.Admin));
            });

            return services;
        }
    
        public static IServiceCollection AddSwagger(this IServiceCollection services)
        {
            services.AddSwaggerGen(setup =>
            {
                var jwtSecurityScheme = new OpenApiSecurityScheme
                {
                    Scheme = "Bearer",
                    BearerFormat = "JWT",
                    Name = "JWT Authentication",
                    In = ParameterLocation.Header,
                    Type = SecuritySchemeType.Http,
                    Description = "Put **_ONLY_** your JWT Bearer token on text-box below!",

                    Reference = new OpenApiReference
                    {
                        Id = JwtBearerDefaults.AuthenticationScheme,
                        Type = ReferenceType.SecurityScheme
                    }
                };

                setup.AddSecurityDefinition(jwtSecurityScheme.Reference.Id, jwtSecurityScheme);

                setup.SwaggerDoc("v1", new OpenApiInfo
                {
                    Version = "v2",
                    Title = "Date-Time Api",
                    Description = "Simple service to get info about delivery dates",
                    Contact = new OpenApiContact
                    {
                        Name = "Vasily Levkovsky",
                        Email = "v.levkovskiy@21vek.by"
                    }
                });
            });

            return services;
        }
    }
}
