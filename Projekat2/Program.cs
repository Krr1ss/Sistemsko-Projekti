using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SistemP_Projekat_Slike_Taskovi
{
    class Program
    {
        // === 1. RAZDVAJANJE PRIJEMA I OBRADE ===
        private static readonly Queue<HttpListenerContext> _requestQueue = new Queue<HttpListenerContext>();
        private static readonly object _queueLock = new object();

        // === 2. THREAD-SAFE KEŠ (Ograničenje veličine) & ASYNC CACHE STAMPEDE ===
        // Koristimo Lazy<Task> da garantujemo da se asinhrona operacija pokreće samo jednom!
        private static readonly ConcurrentDictionary<string, Lazy<Task<byte[]>>> _cache = new ConcurrentDictionary<string, Lazy<Task<byte[]>>>();
        private static readonly ConcurrentQueue<string> _cacheOrder = new ConcurrentQueue<string>();
        private static readonly int MAX_CACHE_SIZE = 50;

        // === 3. KONTROLA PARALELNIH OBRADA ===
        // Ograničavamo server na maksimalno 10 istovremenih taskova za obradu
        private static readonly SemaphoreSlim _throttler = new SemaphoreSlim(10);

        private static readonly object _logLock = new object();
        private static readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private static readonly CountdownEvent _activeRequests = new CountdownEvent(1);
        private static bool _isStopping = false;

        static void Main(string[] args)
        {
            string rootFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Slike");
            if (!Directory.Exists(rootFolder)) Directory.CreateDirectory(rootFolder);

            HttpListener listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:5050/");

            // KLASIČNA NIT: Ostavljena tamo gde Task nema smisla (beskonačno blokiranje na Monitor.Wait)
            Thread dispatcher = new Thread(() => DispatcherLoop(rootFolder));
            dispatcher.IsBackground = true;
            dispatcher.Start();

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                SafeLog("[Server] Pokrenuto graceful gašenje... Čekam taskove da se završe.");
                lock (_queueLock)
                {
                    _isStopping = true;
                    Monitor.PulseAll(_queueLock);
                }
                _cts.Cancel();
                try { listener.Stop(); } catch { }
            };

            try
            {
                listener.Start();
                SafeLog("Server (Async/Tasks) pokrenut na http://localhost:5050/");

                while (!_cts.Token.IsCancellationRequested)
                {
                    // Asinhroni prijem zahteva
                    var getContextTask = listener.GetContextAsync();
                    getContextTask.Wait(_cts.Token); // Čekamo asinhrono dok ne stigne ili se ne ugasi
                    HttpListenerContext context = getContextTask.Result;

                    lock (_queueLock)
                    {
                        if (_isStopping) break;
                        _requestQueue.Enqueue(context);
                        Monitor.Pulse(_queueLock);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (HttpListenerException) { }

            _activeRequests.Signal();
            if (!_activeRequests.Wait(TimeSpan.FromSeconds(5)))
            {
                SafeLog("[Server] Isteklo vreme. Nasilno gašenje.");
                Environment.Exit(0);
            }
            SafeLog("[Server] Server je uspešno ugašen.");
        }

        // Klasična nit koja razvrstava zahteve
        static void DispatcherLoop(string rootFolder)
        {
            while (true)
            {
                HttpListenerContext context = null;

                lock (_queueLock)
                {
                    while (_requestQueue.Count == 0 && !_isStopping)
                    {
                        Monitor.Wait(_queueLock);
                    }
                    if (_isStopping && _requestQueue.Count == 0) break;
                    context = _requestQueue.Dequeue();
                }

                if (context != null)
                {
                    _activeRequests.AddCount();
                    
                    // Čekamo slobodno mesto u semaforu (ograničenje paralelnih obrada)
                    _throttler.Wait(); 

                    // Pokretanje asinhronog Task-a za obradu
                    Task obradaTask = Task.Run(async () =>
                    {
                        await ObradiZahtevAsync(context, rootFolder);
                    });

                    // === 4. DEMONSTRACIJA CONTINUEWITH SINTAKSE ===
                    // Smisleno korišćenje: Logovanje grešaka i oslobađanje resursa NAKON što se glavni task završi.
                    obradaTask.ContinueWith(t =>
                    {
                        if (t.IsFaulted)
                        {
                            SafeLog($"[Task Greška] Došlo je do pucanja: {t.Exception?.GetBaseException().Message}");
                        }
                        
                        // Bez obzira na ishod, moramo da oslobodimo mesto u semaforu i CountdownEvent-u
                        _throttler.Release();
                        _activeRequests.Signal();
                        
                    }, TaskContinuationOptions.ExecuteSynchronously); // Izvršava se odmah čim se prethodni task završi
                }
            }
        }

        static async Task ObradiZahtevAsync(HttpListenerContext context, string rootFolder)
        {
            if (context.Request.HttpMethod != "GET")
            {
                await PosaljiTekstOdgovorAsync(context, "Koristite GET metodu.", 405);
                return;
            }

            string fileName = context.Request.Url.LocalPath.TrimStart('/');
            if (string.IsNullOrEmpty(fileName))
            {
                await PosaljiTekstOdgovorAsync(context, "Niste uneli naziv fajla.", 400);
                return;
            }

            string widthParam = context.Request.QueryString["width"];
            string heightParam = context.Request.QueryString["height"];

            if (string.IsNullOrEmpty(widthParam) || string.IsNullOrEmpty(heightParam) ||
                !int.TryParse(widthParam, out int width) || !int.TryParse(heightParam, out int height))
            {
                await PosaljiTekstOdgovorAsync(context, "Nevalidni ili nedostajući parametri.", 400);
                return;
            }

            string filePath = Path.Combine(rootFolder, fileName);
            if (!File.Exists(filePath))
            {
                await PosaljiTekstOdgovorAsync(context, $"Fajl '{fileName}' ne postoji.", 404);
                return;
            }

            string cacheKey = $"{fileName}_{width}_{height}";

            // Rešavanje Cache Stampede problema na asinhroni način
            // Lazy osigurava da se funkcija ResajzujSlikuAsync prosledi samo jednom, čak i kod konkurentnog pristupa!
            Lazy<Task<byte[]>> lazyImageTask = _cache.GetOrAdd(cacheKey, k => new Lazy<Task<byte[]>>(() => ResajzujSlikuAsync(filePath, width, height, cacheKey)));

            byte[] podaciSlike;
            try
            {
                // Ako je task već gotov, ovo ga odmah vraća (Cache Hit). 
                // Ako nije, nit asinhrono čeka bez blokiranja (rešen stampede).
                podaciSlike = await lazyImageTask.Value;
                SafeLog($"[Task {Task.CurrentId}] Isporučujem sliku -> {cacheKey}");
            }
            catch (Exception)
            {
                // Ako procesiranje pukne, sklanjamo ga iz keša da bi sledeći zahtev mogao da proba ponovo
                _cache.TryRemove(cacheKey, out _);
                await PosaljiTekstOdgovorAsync(context, "Greška pri obradi slike na serveru.", 500);
                return;
            }

            await PosaljiSlikuOdgovorAsync(context, podaciSlike, 200);
        }

        // Pomoćna metoda za samu obradu koja se kešira
        static async Task<byte[]> ResajzujSlikuAsync(string filePath, int width, int height, string cacheKey)
        {
            SafeLog($"[Task {Task.CurrentId}] CACHE MISS -> Obrada slike sa diska: {cacheKey}");
            
            // Image processing u System.Drawing je sinhrono i opterećuje CPU.
            // Zato ga svesno bacamo u poseban pozadinski Task da ne bi blokirali asinhronu mašinu.
            byte[] podaci = await Task.Run(() =>
            {
                using (Image original = Image.FromFile(filePath))
                using (Bitmap resized = new Bitmap(original, new Size(width, height)))
                using (MemoryStream ms = new MemoryStream())
                {
                    resized.Save(ms, ImageFormat.Jpeg);
                    return ms.ToArray();
                }
            });

            OdrzavajVelicinuKesa(cacheKey);
            return podaci;
        }

        static void OdrzavajVelicinuKesa(string noviKljuc)
        {
            _cacheOrder.Enqueue(noviKljuc);

            while (_cache.Count > MAX_CACHE_SIZE)
            {
                if (_cacheOrder.TryDequeue(out string oldestKey))
                {
                    if (_cache.TryRemove(oldestKey, out _))
                    {
                        SafeLog($"[Keš] Izbačen najstariji ključ: {oldestKey}. Velicina: {_cache.Count}");
                    }
                }
            }
        }

        static void SafeLog(string poruka)
        {
            lock (_logLock)
            {
                Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} | {poruka}");
            }
        }

        static async Task PosaljiTekstOdgovorAsync(HttpListenerContext context, string poruka, int statusCode)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(poruka);
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "text/plain; charset=utf-8";
            context.Response.ContentLength64 = buffer.Length;
            try { await context.Response.OutputStream.WriteAsync(buffer, 0, buffer.Length); } catch { }
            finally { try { context.Response.OutputStream.Close(); } catch { } }
        }

        static async Task PosaljiSlikuOdgovorAsync(HttpListenerContext context, byte[] podaciSlike, int statusCode)
        {
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "image/jpeg";
            context.Response.ContentLength64 = podaciSlike.Length;
            try { await context.Response.OutputStream.WriteAsync(podaciSlike, 0, podaciSlike.Length); } catch { }
            finally { try { context.Response.OutputStream.Close(); } catch { } }
        }
    }
}