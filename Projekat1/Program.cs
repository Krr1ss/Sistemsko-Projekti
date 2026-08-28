using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;

namespace SistemP_Projekat_Slike
{
    class Program
    {
        // === 1. RAZDVAJANJE PRIJEMA I OBRADE (DELJENI RED) ===
        private static readonly Queue<HttpListenerContext> _requestQueue = new Queue<HttpListenerContext>();
        private static readonly object _queueLock = new object();

        // === 2. THREAD-SAFE KEŠ (SA OGRANIČENJEM VELIČINE - FIFO) ===
        private static readonly ConcurrentDictionary<string, byte[]> _cache = new ConcurrentDictionary<string, byte[]>();
        private static readonly ConcurrentQueue<string> _cacheOrder = new ConcurrentQueue<string>();
        private static readonly int MAX_CACHE_SIZE = 50;

        // Striped locking za Cache Stampede
        private static readonly ConcurrentDictionary<string, object> _imageLocks = new ConcurrentDictionary<string, object>();

        // Katanac za Thread-Safe logovanje u konzolu
        private static readonly object _logLock = new object();

        // === 3. GRACEFUL SHUTDOWN ===
        private static readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private static readonly CountdownEvent _activeRequests = new CountdownEvent(1);
        private static bool _isStopping = false;

        static void Main(string[] args)
        {
            string rootFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Slike");
            if (!Directory.Exists(rootFolder))
            {
                Directory.CreateDirectory(rootFolder);
            }

            HttpListener listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:5050/");

            // Pokrećemo pozadinsku nit koja stalno nadgleda red i delegira posao ThreadPool-u
            Thread dispatcher = new Thread(() => DispatcherLoop(rootFolder));
            dispatcher.IsBackground = true;
            dispatcher.Start();

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                SafeLog("[Server] Pokrenuto graceful gašenje... Čekam niti.");
                
                lock (_queueLock)
                {
                    _isStopping = true;
                    Monitor.PulseAll(_queueLock); // Budi dispatcher nit ako spava
                }

                _cts.Cancel();
                try { listener.Stop(); } catch { }
            };

            try
            {
                listener.Start();
                SafeLog("Server pokrenut na http://localhost:5050/ (Pritisni Ctrl+C za kraj)");

                while (!_cts.Token.IsCancellationRequested)
                {
                    HttpListenerContext context = listener.GetContext();

                    lock (_queueLock)
                    {
                        if (_isStopping) break;

                        // Smeštanje u deljenu strukturu podataka
                        _requestQueue.Enqueue(context);
                        
                        // BLOKIRAJUĆA SINHRONIZACIJA: Signalizacija preko Monitor.Pulse
                        Monitor.Pulse(_queueLock); 
                    }
                }
            }
            catch (HttpListenerException) { }

            // Čekanje završetka aktivnih obrada (Timeout osigurač od 5s)
            _activeRequests.Signal();
            if (!_activeRequests.Wait(TimeSpan.FromSeconds(5)))
            {
                SafeLog("[Server] Isteklo vreme čekanja. Nasilno gašenje.");
                Environment.Exit(0);
            }

            SafeLog("[Server] Server je uspešno ugašen.");
        }

        // Ova nit uzima zahteve iz reda koristeći Monitor.Wait/Pulse i šalje ih ThreadPool-u
        static void DispatcherLoop(string rootFolder)
        {
            while (true)
            {
                HttpListenerContext context = null;

                lock (_queueLock)
                {
                    // BLOKIRAJUĆA SINHRONIZACIJA: Monitor.Wait ako nema posla
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

                    // Korišćenje ThreadPool-a za kontrolisanu paralelnu obradu
                    ThreadPool.QueueUserWorkItem(state =>
                    {
                        try
                        {
                            ObradiZahtev((HttpListenerContext)state, rootFolder);
                        }
                        finally
                        {
                            _activeRequests.Signal();
                        }
                    }, context);
                }
            }
        }

        static void ObradiZahtev(HttpListenerContext context, string rootFolder)
        {
            if (context.Request.HttpMethod != "GET")
            {
                PosaljiTekstOdgovor(context, "Koristite GET metodu.", 405);
                return;
            }

            string fileName = context.Request.Url.LocalPath.TrimStart('/');
            if (string.IsNullOrEmpty(fileName))
            {
                PosaljiTekstOdgovor(context, "Niste uneli naziv fajla.", 400);
                return;
            }

            string widthParam = context.Request.QueryString["width"];
            string heightParam = context.Request.QueryString["height"];

            if (string.IsNullOrEmpty(widthParam) || string.IsNullOrEmpty(heightParam) ||
                !int.TryParse(widthParam, out int width) || !int.TryParse(heightParam, out int height))
            {
                PosaljiTekstOdgovor(context, "Nevalidni ili nedostajući parametri dimenzija.", 400);
                return;
            }

            string filePath = Path.Combine(rootFolder, fileName);
            if (!File.Exists(filePath))
            {
                PosaljiTekstOdgovor(context, $"Fajl '{fileName}' ne postoji.", 404);
                return;
            }

            string cacheKey = $"{fileName}_{width}_{height}";
            byte[] podaciSlike = null;

            // 1. PROVERA KEŠA (Brzo čitanje)
            if (_cache.TryGetValue(cacheKey, out podaciSlike))
            {
                SafeLog($"[{Thread.CurrentThread.ManagedThreadId}] CACHE HIT -> {cacheKey}");
            }
            else
            {
                // REŠAVANJE CACHE STAMPEDE (Striped locking)
                object slikaLock = _imageLocks.GetOrAdd(cacheKey, _ => new object());

                lock (slikaLock)
                {
                    // 2. DVOSTRUKA PROVERA (Double-Check)
                    if (_cache.TryGetValue(cacheKey, out podaciSlike))
                    {
                        SafeLog($"[{Thread.CurrentThread.ManagedThreadId}] CACHE HIT (Spasen lock-om!) -> {cacheKey}");
                    }
                    else
                    {
                        SafeLog($"[{Thread.CurrentThread.ManagedThreadId}] CACHE MISS -> {cacheKey} (Obrada sa diska)");

                        try
                        {
                            using (Image original = Image.FromFile(filePath))
                            using (Bitmap resized = new Bitmap(original, new Size(width, height)))
                            using (MemoryStream ms = new MemoryStream())
                            {
                                resized.Save(ms, ImageFormat.Jpeg);
                                podaciSlike = ms.ToArray();
                            }

                            UpisiUKes(cacheKey, podaciSlike);
                        }
                        catch (Exception ex)
                        {
                            SafeLog($"Greska: {ex.Message}");
                            PosaljiTekstOdgovor(context, "Greška na serveru pri obradi slike.", 500);
                            return;
                        }
                    }
                }
            }

            PosaljiSlikuOdgovor(context, podaciSlike, 200);
        }

        static void UpisiUKes(string key, byte[] data)
        {
            _cache[key] = data;
            _cacheOrder.Enqueue(key);

            while (_cache.Count > MAX_CACHE_SIZE)
            {
                if (_cacheOrder.TryDequeue(out string oldestKey))
                {
                    if (_cache.TryRemove(oldestKey, out _))
                    {
                        SafeLog($"[Keš] Izbačen najstariji ključ: {oldestKey}. Velicina keša: {_cache.Count}");
                        _imageLocks.TryRemove(oldestKey, out _);
                    }
                }
            }
        }

        // === THREAD-SAFE LOGOVANJE ===
        static void SafeLog(string poruka)
        {
            lock (_logLock)
            {
                Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} | {poruka}");
            }
        }

        static void PosaljiTekstOdgovor(HttpListenerContext context, string poruka, int statusCode)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(poruka);
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "text/plain; charset=utf-8";
            context.Response.ContentLength64 = buffer.Length;
            try { context.Response.OutputStream.Write(buffer, 0, buffer.Length); } catch { }
            finally { try { context.Response.OutputStream.Close(); } catch { } }
        }

        static void PosaljiSlikuOdgovor(HttpListenerContext context, byte[] podaciSlike, int statusCode)
        {
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "image/jpeg";
            context.Response.ContentLength64 = podaciSlike.Length;
            try { context.Response.OutputStream.Write(podaciSlike, 0, podaciSlike.Length); } catch { }
            finally { try { context.Response.OutputStream.Close(); } catch { } }
        }
    }
}