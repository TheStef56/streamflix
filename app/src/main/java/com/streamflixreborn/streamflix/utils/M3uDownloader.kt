package com.streamflixreborn.streamflix.utils

import android.content.ContentValues
import android.util.Log
import com.streamflixreborn.streamflix.fragments.player.PlayerMobileFragment
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import android.content.Context
import android.os.Build
import android.os.Environment
import android.provider.MediaStore
import androidx.annotation.RequiresApi
import android.net.Uri
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.File
import java.util.Collections
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

@RequiresApi(Build.VERSION_CODES.Q)
class M3uDownloader(
    private val context: Context,
    val player : PlayerMobileFragment,
    private val client: OkHttpClient = OkHttpClient(),
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
) {

    private val parallelism = 16
    private val retryLimit = 3

    private var restartWatcher = true

    @Volatile private var bestAudio: String = ""
    @Volatile private var bestVideo: String = ""

    private val refreshMutex = Mutex()

    private val queue = Channel<Segment>(Channel.UNLIMITED)

    private val tempDir: File by lazy {
        File(context.cacheDir, "hls/session_${System.currentTimeMillis()}").apply {
            mkdirs()
        }
    }

    private val audioFiles = Collections.synchronizedList(mutableListOf<File>())
    private val videoFiles = Collections.synchronizedList(mutableListOf<File>())
    private val audioOutputFile = File(tempDir, "audio.ts")
    private val videoOutputFile = File(tempDir, "video.ts")

    private var aesKey: ByteArray? = null
    private var aesIv: ByteArray? = null
    private var keyUrl: String? = "https://vixcloud.co/storage/enc.key"  // TODO: hostname dynamically

    @Volatile private var audioSegToDownload = 0
    @Volatile private var videoSegToDownload = 0
    data class Segment(
        val index: Int,
        var url: String,
        val file: File,
        var attempts: Int = 0
    )
    fun start() {
        startWorkers()
        scope.launch {
            refreshPlaylistsAndEnqueue()
        }
        startCompletionWatcher()
    }

    private fun startWorkers() {
        repeat(parallelism) {
            scope.launch {
                for (segment in queue) {
                    downloadSegmentSafe(segment)
                }
            }
        }
    }

    private fun ensureKeyLoaded(baseUrl: String) {
        if (aesKey != null) return
        if (keyUrl == null) return

        val resolved = if (keyUrl!!.startsWith("http")) {
            keyUrl!!
        } else {
            baseUrl.substringBeforeLast("/") + keyUrl!!
        }

        val request = Request.Builder().url(resolved).build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw RuntimeException("Key download failed")
            }
            aesKey = response.body!!.bytes()
        }
    }

    private suspend fun downloadSegmentSafe(segment: Segment) {
        try {
            downloadSegment(segment.url, segment.file)

//            Log.d("HLS", "Downloaded segment ${segment.index}")

            synchronized(this) {
                if (segment.file.name.startsWith("audio")) {
                    audioFiles.add(segment.file)
                } else {
                    videoFiles.add(segment.file)
                }
            }

        } catch (e: Exception) {
            segment.attempts++

            val isAuthError = e.message?.contains("401") == true ||
                    e.message?.contains("403") == true

            if (isAuthError) {
                handleTokenExpiry()
                queue.send(segment.copy(url = segment.url))
                return
            }

            if (segment.attempts < retryLimit) {
                queue.send(segment)
            } else {
                Log.e("HLS", "Failed segment ${segment.index}")
            }
        }
    }

    private fun downloadSegment(url: String, file: File) {
        val request = Request.Builder().url(url).build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw RuntimeException("HTTP ${response.code}")
            }

            val body = response.body ?: throw RuntimeException("Empty body")

            ensureKeyLoaded(url)

            val encrypted = body.bytes()

            val decrypted = decryptAes128(
                encrypted,
                aesKey!!,
                aesIv ?: ByteArray(16)
            )

            file.outputStream().use { it.write(decrypted) }
        }
    }

    private suspend fun refreshPlaylistsAndEnqueue() {
        refreshMutex.withLock {

            Log.d("HLS", "Refreshing playlists...")

            // 1. fetch master → update bestAudio/bestVideo
            resolveStreams()

            // 2. fetch and parse playlists
            val audioSegments = parsePlaylist(bestAudio, "audio")
            val videoSegments = parsePlaylist(bestVideo, "video")
            audioSegToDownload = audioSegments.size
            videoSegToDownload = videoSegments.size

            // 3. enqueue both streams
            enqueueSegments(audioSegments)
            enqueueSegments(videoSegments)
        }
    }

    private fun hexToBytes(hex: String): ByteArray {
        val clean = if (hex.startsWith("0x")) hex.substring(2) else hex
        return clean.chunked(2)
            .map { it.toInt(16).toByte() }
            .toByteArray()
    }

    private fun resolveStreams() {
        val uri = player.currentVideo?.source.toString()
        val decoded = player.decodeBase64Uri(uri)

        val lines = decoded?.lines()
        lines?.forEachIndexed { idx, line ->
            if (line.contains("GROUP-ID=\"audio\"") && line.contains("DEFAULT=YES")) {
                bestAudio = lines[idx].split("URI=\"")[1].split("\"")[0]
            }
            if (line.contains("RESOLUTION=1920x1080")) {
                bestVideo = lines[idx + 1]
            }
        }
        Log.d("BEST-AUDIO", bestAudio)
        Log.d("BEST-VIDEO",bestVideo)
    }

    private fun decryptAes128(data: ByteArray, key: ByteArray, iv: ByteArray): ByteArray {
        val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
        val secretKey = SecretKeySpec(key, "AES")
        val ivSpec = IvParameterSpec(iv)

        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivSpec)
        return cipher.doFinal(data)
    }

    private fun parsePlaylist(url: String, type: String): List<Segment> {
        val request = Request.Builder().url(url).build()

        val playlistText = client.newCall(request).execute().use {
            if (!it.isSuccessful) throw RuntimeException("Playlist failed")
            it.body?.string() ?: ""
        }

        val lines = playlistText.split("\n")
            .filter { it.isNotBlank() && !it.startsWith("#") }

        lines.forEach { line ->
            if (line.startsWith("#EXT-X-KEY")) {
                val ivMatch = Regex("IV=0x([0-9A-Fa-f]+)").find(line)

                ivMatch?.groupValues?.get(1)?.let { aesIv = hexToBytes(it) }
            }
        }

        return lines.mapIndexedNotNull { index, segmentUrl ->

            val file = File(tempDir, "$type-$index.ts")

            // SKIP if already downloaded on disk
            if (file.exists() && file.length() > 0) {
                null
            } else {
                Segment(
                    index = index,
                    url = segmentUrl,
                    file = file
                )
            }
        }
    }

    private suspend fun enqueueSegments(segments: List<Segment>) {
        for (s in segments) {
            queue.send(s)
        }
    }

    private suspend fun handleTokenExpiry() {
        refreshMutex.withLock {
            restartWatcher = true
            Log.d("HLS", "Token expired → refreshing playlists")

            queue.close()

            val newQueue = Channel<Segment>(Channel.UNLIMITED)

            resolveStreams()

            // replace queue reference safely (simple restart model)
            val audio = parsePlaylist(bestAudio, "audio")
            val video = parsePlaylist(bestVideo, "video")
            audioSegToDownload = audio.size
            videoSegToDownload = video.size

            audio.forEach { newQueue.send(it) }
            video.forEach { newQueue.send(it) }

            startWorkersFromQueue(newQueue)
            startCompletionWatcher()
        }
    }

    private fun startWorkersFromQueue(newQueue: Channel<Segment>) {
        repeat(parallelism) {
            scope.launch {
                for (segment in newQueue) {
                    downloadSegmentSafe(segment)
                }
            }
        }
    }

    fun mergeTsSegments(
        segmentFiles: List<File>,
        outputFile: File
    ) {
        outputFile.outputStream().use { output ->
            for (file in segmentFiles.sortedBy {
                it.name.substringAfter("-")
                    .substringBefore(".")
                    .toInt()
            }) {
                file.inputStream().use { input ->
                    input.copyTo(output)
                }
            }
        }
    }

    fun copyToDownloads(context: Context, file: File, out: String): Uri {
        val resolver = context.contentResolver

        val values = ContentValues().apply {
            put(MediaStore.MediaColumns.DISPLAY_NAME, out)
            put(MediaStore.MediaColumns.MIME_TYPE, "video/mp2t")
            put(MediaStore.MediaColumns.RELATIVE_PATH, Environment.DIRECTORY_DOWNLOADS)
        }

        val uri = resolver.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, values)
            ?: throw RuntimeException("Insert failed")

        resolver.openOutputStream(uri)?.use { output ->
            file.inputStream().use { input ->
                input.copyTo(output)
            }
        }
        return uri
    }

    fun buildFinalStreams() {
        scope.launch(Dispatchers.IO) {

            Log.d("HLS", "Merging audio segments...")
            mergeTsSegments(audioFiles, audioOutputFile)

            Log.d("HLS", "Merging video segments...")
            mergeTsSegments(videoFiles, videoOutputFile)

            Log.d("HLS", "Done: ${audioOutputFile.path} + ${videoOutputFile.path}")

            Log.d("TS", "audio exists=${audioOutputFile.exists()} size=${audioOutputFile.length()}")
            Log.d("TS", "video exists=${videoOutputFile.exists()} size=${videoOutputFile.length()}")

            copyToDownloads(context.applicationContext, audioOutputFile, "audio.ts")
            copyToDownloads(context.applicationContext, videoOutputFile, "video.ts")

            // TODO: find a convenient way to merge them into an mp4

        }
    }

    private fun startCompletionWatcher() {
        if (restartWatcher) {
            restartWatcher = false
        }
        scope.launch {
            while (true) {
                if (restartWatcher) break
                delay(1000)

                if (audioFiles.size == audioSegToDownload && videoFiles.size == videoSegToDownload) {
                    Log.d("HLS", "Download complete → merging streams")

                    buildFinalStreams()
                    break
                }

                Log.d("AUDIO/VIDEO:", "${audioFiles.size}/$audioSegToDownload ${videoFiles.size}/$videoSegToDownload" )
            }
        }
    }

}