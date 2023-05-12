using Dapper;
using DateTimeService.Application.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml.Serialization;

namespace DateTimeService.Application.Repositories
{
    public interface IGeoZones
    {
        Task<string> GetGeoZoneID(AddressCoordinates coords);

        Task<AddressCoordinates> GetAddressCoordinates(string address_id);

        Task<Boolean> AdressExists(SqlConnection connection, string _addressId, CancellationToken token = default);

        Task<(bool, string)> CheckAddressGeozone(IntervalListQuery query, SqlConnection connection, CancellationToken token = default);
    }

    public class GeoZones: IGeoZones
    {
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        public GeoZones(IConfiguration configuration, IHttpClientFactory httpClientFactory)
        {
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
        }

        public async Task<Boolean> AdressExists(SqlConnection connection, string _addressId, CancellationToken token = default)
        {
            bool result;
            try
            {
                string query = """
                    Select Top 1 --по адресу находим геозону
                    	1 
                    From dbo._Reference112 ГеоАдрес With (NOLOCK)
                    Where ГеоАдрес._Fld25155 = @AddressId 
                        And ГеоАдрес._Marked = 0x00
                        And ГеоАдрес._Fld2785RRef <> 0x00000000000000000000000000000000
                    """
                ;

                var queryResult = await connection.QueryAsync<int>(new CommandDefinition(query, new{ AddressId = _addressId }, cancellationToken: token));

                result = queryResult != null && queryResult.Any();
            }
            catch (Exception)
            {
                throw;
            }

            return result;
        }

        public async Task<AddressCoordinates> GetAddressCoordinates(string address_id)
        {

            AddressCoordinates result = new();

            var client = _httpClientFactory.CreateClient();

            client.Timeout = new TimeSpan(0, 0, 8);
            client.DefaultRequestHeaders.Add("Accept", "application/vnd.api+json");
            
            string connString = _configuration.GetConnectionString("api21vekby_location");

            var uri = new Uri(connString + address_id);
            HttpRequestMessage request = new(HttpMethod.Get, uri)
            {
                Content = new StringContent("{}", Encoding.UTF8, "application/vnd.api+json")
            };

            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/vnd.api+json");

            try
            {
                var response = await client.SendAsync(request);
                if (response.IsSuccessStatusCode)
                {
                    var responseString = await response.Content.ReadAsStringAsync();

                    IFormatProvider formatter = new NumberFormatInfo { NumberDecimalSeparator = "." };

                    var locationsResponse = JsonSerializer.Deserialize<LocationsResponse>(responseString);

                    foreach (var item in locationsResponse.Data)
                    {

                        result = new(item.Attributes.X_coordinate, item.Attributes.Y_coordinate);

                        break;
                    }
                }
            }
            catch (FormatException)
            {
                throw;
            }
            catch (Exception)
            {
                throw;
            }

            return result;
        }

        public async Task<string> GetGeoZoneID(AddressCoordinates coords)
        {
            string connString = _configuration.GetConnectionString("BTS_zones");
            string login = _configuration.GetValue<string>("BTS_login");
            string pass = _configuration.GetValue<string>("BTS_pass");

            var client = _httpClientFactory.CreateClient();

            client.Timeout = new TimeSpan(0, 0, 8);

            var request = new HttpRequestMessage(HttpMethod.Post,
            connString);
            
            IFormatProvider formatter = new NumberFormatInfo { NumberDecimalSeparator = "." };
            string content = @"
                <soap:Envelope xmlns:soap=""http://schemas.xmlsoap.org/soap/envelope/"">
                    <soap:Body>
                        <m:getZoneByCoords xmlns:m=""http://ws.vrptwserver.beltranssat.by/"">
                            <m:latitude xmlns:xs=""http://www.w3.org/2001/XMLSchema"" 
                     xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"">{0}</m:latitude>
                            <m:longitude xmlns:xs=""http://www.w3.org/2001/XMLSchema"" 
                     xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"">{1}</m:longitude>
                            <m:geomNeeded xmlns:xs=""http://www.w3.org/2001/XMLSchema"" 
                     xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"">false</m:geomNeeded>
                        </m:getZoneByCoords>
                    </soap:Body>
                </soap:Envelope>";

            content = string.Format(content, coords.X_coordinates.ToString(formatter), coords.Y_coordinates.ToString(formatter));

            request.Content = new StringContent(content, Encoding.UTF8, "text/xml");

            var authenticationString = login + ":" + pass;
            var base64EncodedAuthenticationString = Convert.ToBase64String(Encoding.ASCII.GetBytes(authenticationString));

            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", base64EncodedAuthenticationString);
            string result = "";
            try
            {
                var response = await client.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    using var responseStream = await response.Content.ReadAsStreamAsync();
                    var xml = new XmlSerializer(typeof(Envelope));
                    var responseData = (Envelope)xml.Deserialize(responseStream);
                    result = responseData.Items[0].getZoneByCoordsResponse.zone.id;
                }
                else
                {
                    throw new Exception(response.ToString());
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return result;
        }

        public async Task<(bool, string)> CheckAddressGeozone(IntervalListQuery query, SqlConnection connection, CancellationToken token = default)
        {
            bool addressExists = false;
            string zoneId = "";

            bool checkByOrder = !String.IsNullOrEmpty(query.OrderNumber) && query.OrderDate != default;
            bool alwaysCheckGeozone = false;

            if (query.DeliveryType == Constants.Self || checkByOrder)
            {
                addressExists = true;
            }
            else
            {
                alwaysCheckGeozone = _configuration.GetValue<bool>("AlwaysCheckGeozone");
                if (!alwaysCheckGeozone)
                {
                    addressExists = await AdressExists(connection, query.AddressId, token);
                }
            }

            if (!addressExists || alwaysCheckGeozone)
            {
                AddressCoordinates coords;

                if (!String.IsNullOrEmpty(query.Xcoordinate) && !String.IsNullOrEmpty(query.Ycoordinate))
                {
                    coords = new(query.Xcoordinate, query.Ycoordinate);
                }
                else
                {
                    coords = await GetAddressCoordinates(query.AddressId);
                }

                if (coords.AvailableToUse)
                {
                    zoneId = await GetGeoZoneID(coords);
                }
            }

            return (addressExists, zoneId);
        }
    }

    public class LocationsResponse
    {
        [JsonPropertyName("data")]
        public List<LocationsResponseElement> Data { get; set; }
    }

    public class LocationsResponseElement
    {
        [JsonPropertyName("attributes")]
        public LocationsResponseElemAttribute Attributes { get; set; }
        [JsonPropertyName("type")]
        public string Type { get; set; }
        [JsonPropertyName("id")]
        public decimal Id { get; set; }
    }

    public class LocationsResponseElemAttribute
    {
        [JsonPropertyName("x_coordinate")]
        public string X_coordinate { get; set; }
        [JsonPropertyName("y_coordinate")]
        public string Y_coordinate { get; set; }
    }
}
