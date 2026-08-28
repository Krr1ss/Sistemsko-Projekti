using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;

namespace SistemP_Projekat_AkkaRx
{
    // === 1. MODELI PODATAKA ===
    public class MatchDto
    {
        public string MatchId { get; set; } // BITNO: Da ne bismo duplo brojali mečeve pri periodičnom skidanju
        public string Team1 { get; set; }
        public string Team2 { get; set; }
        public string Winner { get; set; }
    }

    public class TeamStats
    {
        public string TeamName { get; set; }
        public int Wins { get; set; }
        public int Losses { get; set; }
        public double WinPercentage => (Wins + Losses) == 0 ? 0 : Math.Round((double)Wins / (Wins + Losses) * 100, 2);
    }

    // === 2. AKKA PORUKE ===
    public record GetTournamentStatsRequest(string TournamentId);
    public record MatchMessage(MatchDto Match, string TournamentId);
    public record ErrorMessage(string Error, string TournamentId);
    public record StatsResponseMessage(string TournamentId, List<TeamStats> Stats, string Message = "");

    // === 3. Rx.NET SERVIS (PERIODIČNO DOHVATANJE - ZAHTEV ASISTENTA) ===
    public static class RxApiService
    {
        // Pokreće beskonačni strim koji skida podatke svakih 15 sekundi
        public static void StartPeriodicFetching(string tournamentId, IActorRef targetActor)
        {
            // Observable.Timer(0, 15s) znači: "Okini odmah prvi put, a onda svake 15. sekunde"
            Observable.Timer(TimeSpan.Zero, TimeSpan.FromSeconds(15))
                .SelectMany(_ => Observable.FromAsync(async () =>
                {
                    try
                    {
                        // TODO: Ovde ide pravi HttpClient poziv ka TourneyRadar API-ju
                        // Simuliramo mrežno kašnjenje i vraćanje podataka
                        await Task.Delay(500);
                        
                        string mockJson = @"[
                            { ""MatchId"": ""m1"", ""Team1"": ""Navi"", ""Team2"": ""FaZe"", ""Winner"": ""Navi"" },
                            { ""MatchId"": ""m2"", ""Team1"": ""FaZe"", ""Team2"": ""G2"", ""Winner"": ""FaZe"" },
                            { ""MatchId"": ""m3"", ""Team1"": ""Cloud9"", ""Team2"": ""Navi"", ""Winner"": ""Cloud9"" },
                            { ""MatchId"": ""m-invalid"", ""Team1"": ""InvalidTeam"", ""Team2"": ""Unknown"", ""Winner"": """" } 
                        ]";
                        
                        return JsonSerializer.Deserialize<List<MatchDto>>(mockJson);
                    }
                    catch (Exception ex)
                    {
                        // Ako pukne mreža, prijavljujemo Aktoru, ali NE DOZVOLJAVAMO da strim umre!
                        targetActor.Tell(new ErrorMessage(ex.Message, tournamentId));
                        return new List<MatchDto>(); 
                    }
                }))
                .SubscribeOn(TaskPoolScheduler.Default) // Multithreading poen
                .SelectMany(listaMeceva => listaMeceva) // Ravna listu u pojedinačne mečeve
                .Where(m => !string.IsNullOrEmpty(m.Winner)) // Filtriranje: samo završeni mečevi
                .Subscribe(match => 
                {
                    // Emitovanje pojedinačnog meča Aktoru
                    targetActor.Tell(new MatchMessage(match, tournamentId));
                });
        }
    }

    // === 4. AKKA.NET AKTOR (KONTINUIRANO ODRŽAVANJE STANJA) ===
    public class TournamentActor : ReceiveActor
    {
        // Glavno stanje Aktora: pamti statistiku za turnire
        private readonly Dictionary<string, Dictionary<string, TeamStats>> _tournamentStats = new();
        
        // Evidencija obrađenih mečeva da ne bismo sabirali iste pobede iznova svakih 15 sekundi
        private readonly HashSet<string> _processedMatches = new();
        
        // Evidencija za koje turnire smo već upalili Rx Timer
        private readonly HashSet<string> _activeRxTimers = new();

        public TournamentActor()
        {
            // Web Server traži TRENUTNO stanje
            Receive<GetTournamentStatsRequest>(msg =>
            {
                ServerLogger.SafeLog($"[Aktor] Klijent traži stanje za turnir: {msg.TournamentId}");

                // Ako prvi put čujemo za ovaj turnir, inicijalizujemo ga i palimo Rx Timer u pozadini
                if (!_activeRxTimers.Contains(msg.TournamentId))
                {
                    ServerLogger.SafeLog($"[Aktor] Prvi upit za {msg.TournamentId}. Pokrećem pozadinski Rx periodic polling...");
                    _activeRxTimers.Add(msg.TournamentId);
                    _tournamentStats[msg.TournamentId] = new Dictionary<string, TeamStats>();
                    
                    RxApiService.StartPeriodicFetching(msg.TournamentId, Self);
                    
                    // Vraćamo odmah klijentu poruku da se podaci tek učitavaju (ne blokiramo ga!)
                    Sender.Tell(new StatsResponseMessage(msg.TournamentId, new List<TeamStats>(), "Podaci se trenutno prikupljaju. Osvezite stranicu za par sekundi."));
                    return;
                }

                // Vraća trenutno stanje onakvo kakvo jeste (Zahtev asistenta)
                var currentStats = _tournamentStats[msg.TournamentId].Values.ToList();
                Sender.Tell(new StatsResponseMessage(msg.TournamentId, currentStats));
            });

            // Ažuriranje stanja na osnovu Rx.NET poruka
            Receive<MatchMessage>(msg =>
            {
                // Zaštita od dupliranja (jer API periodično vraća i stare mečeve)
                if (_processedMatches.Contains(msg.Match.MatchId)) return;

                _processedMatches.Add(msg.Match.MatchId);
                var stats = _tournamentStats[msg.TournamentId];
                var m = msg.Match;

                if (!stats.ContainsKey(m.Team1)) stats[m.Team1] = new TeamStats { TeamName = m.Team1 };
                if (!stats.ContainsKey(m.Team2)) stats[m.Team2] = new TeamStats { TeamName = m.Team2 };

                if (m.Winner == m.Team1)
                {
                    stats[m.Team1].Wins++;
                    stats[m.Team2].Losses++;
                }
                else if (m.Winner == m.Team2)
                {
                    stats[m.Team2].Wins++;
                    stats[m.Team1].Losses++;
                }
                
                ServerLogger.SafeLog($"[Aktor] Ažurirano stanje za meč {m.MatchId} ({m.Team1} vs {m.Team2})");
            });

            Receive<ErrorMessage>(msg =>
            {
                ServerLogger.SafeLog($"[Aktor] GREŠKA iz Rx.NET-a za turnir {msg.TournamentId}: {msg.Error}");
            });
        }
    }

    // === 5. THREAD-SAFE LOGGER ===
    public static class ServerLogger
    {
        private static readonly object _logLock = new object();
        public static void SafeLog(string message)
        {
            lock (_logLock)
            {
                Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} | {message}");
            }
        }
    }

    // === 6. MAIN - WEB SERVER ===
    class Program
    {
        static async Task Main(string[] args)
        {
            // Bonus poeni: Custom Dispatcher
            var config = ConfigurationFactory.ParseString(@"
                akka.actor.custom-dispatcher {
                    type = Dispatcher
                    executor = thread-pool-executor
                    thread-pool-executor { core-pool-size-min = 2, core-pool-size-max = 10 }
                }");

            using var system = ActorSystem.Create("EsportsSystem", config);
            var tournamentActor = system.ActorOf(Props.Create<TournamentActor>().WithDispatcher("akka.actor.custom-dispatcher"), "tournamentStats");

            HttpListener listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:5050/");
            listener.Start();
            
            ServerLogger.SafeLog("[Server] Pokrenut na http://localhost:5050/");
            ServerLogger.SafeLog("[Server] Testiraj u browseru: http://localhost:5050/?tournamentId=IEM_Katowice");

            while (true)
            {
                var context = await listener.GetContextAsync();
                _ = ProcessRequestAsync(context, tournamentActor);
            }
        }

        static async Task ProcessRequestAsync(HttpListenerContext context, IActorRef tournamentActor)
        {
            try
            {
                string tourneyId = context.Request.QueryString["tournamentId"];
                if (string.IsNullOrEmpty(tourneyId))
                {
                    await PosaljiOdgovor(context, "{\"error\": \"Nedostaje parametar 'tournamentId'\"}", 400);
                    return;
                }

                // Web server NIKADA NE ČEKA Rx.NET. Pita Aktora šta ima trenutno i odmah vraća!
                var response = await tournamentActor.Ask<StatsResponseMessage>(
                    new GetTournamentStatsRequest(tourneyId), 
                    TimeSpan.FromSeconds(5)); 

                var jsonOptions = new JsonSerializerOptions { WriteIndented = true };
                string json = JsonSerializer.Serialize(new { 
                    Tournament = response.TournamentId, 
                    Message = response.Message,
                    Stats = response.Stats 
                }, jsonOptions);
                
                await PosaljiOdgovor(context, json, 200);
            }
            catch (Exception ex)
            {
                ServerLogger.SafeLog($"[HTTP] Greška: {ex.Message}");
                await PosaljiOdgovor(context, "{\"error\": \"Greška na serveru.\"}", 500);
            }
        }

        static async Task PosaljiOdgovor(HttpListenerContext context, string jsonContent, int statusCode)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(jsonContent);
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "application/json; charset=utf-8";
            context.Response.ContentLength64 = buffer.Length;
            context.Response.AppendHeader("Access-Control-Allow-Origin", "*");

            try { await context.Response.OutputStream.WriteAsync(buffer, 0, buffer.Length); } catch { }
            finally { try { context.Response.OutputStream.Close(); } catch { } }
        }
    }
}