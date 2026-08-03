param(
    [string] $JarPath = (Join-Path $PSScriptRoot '..\centrol.jar')
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$nestedJarEntryName = 'BOOT-INF/lib/spring-kafka-test-2.8.10.jar'
$zookeeperClassEntryName = 'org/springframework/kafka/test/EmbeddedKafkaBroker$EmbeddedZookeeper.class'
$oldTickSequence = [byte[]](0x11, 0x03, 0x20, 0xB7)
$newTickSequence = [byte[]](0x11, 0x0B, 0xB8, 0xB7)

function Read-ZipEntryBytes {
    param(
        [string] $ArchivePath,
        [string] $EntryName
    )

    $archive = [System.IO.Compression.ZipFile]::OpenRead(
        (Resolve-Path -LiteralPath $ArchivePath).Path
    )
    try {
        $entry = $archive.GetEntry($EntryName)
        if ($null -eq $entry) {
            throw "Missing archive entry: $EntryName"
        }
        $memoryStream = [System.IO.MemoryStream]::new()
        $entryStream = $entry.Open()
        try {
            $entryStream.CopyTo($memoryStream)
            return $memoryStream.ToArray()
        }
        finally {
            $entryStream.Dispose()
            $memoryStream.Dispose()
        }
    }
    finally {
        $archive.Dispose()
    }
}

function Replace-ZipEntry {
    param(
        [string] $ArchivePath,
        [string] $EntryName,
        [byte[]] $Content,
        [System.IO.Compression.CompressionLevel] $CompressionLevel
    )

    $fileStream = [System.IO.File]::Open(
        (Resolve-Path -LiteralPath $ArchivePath).Path,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::None
    )
    $archive = [System.IO.Compression.ZipArchive]::new(
        $fileStream,
        [System.IO.Compression.ZipArchiveMode]::Update,
        $false
    )
    try {
        $entry = $archive.GetEntry($EntryName)
        if ($null -eq $entry) {
            throw "Missing archive entry: $EntryName"
        }
        $entry.Delete()
        $replacementEntry = $archive.CreateEntry($EntryName, $CompressionLevel)
        $replacementStream = $replacementEntry.Open()
        try {
            $replacementStream.Write($Content, 0, $Content.Length)
        }
        finally {
            $replacementStream.Dispose()
        }
    }
    finally {
        $archive.Dispose()
        $fileStream.Dispose()
    }
}

function Find-SequenceOffsets {
    param(
        [byte[]] $Content,
        [byte[]] $Sequence
    )

    $offsetList = [System.Collections.Generic.List[int]]::new()
    for ($offset = 0; $offset -le $Content.Length - $Sequence.Length; $offset++) {
        $matched = $true
        for ($index = 0; $index -lt $Sequence.Length; $index++) {
            if ($Content[$offset + $index] -ne $Sequence[$index]) {
                $matched = $false
                break
            }
        }
        if ($matched) {
            $offsetList.Add($offset)
        }
    }
    return $offsetList.ToArray()
}

$temporaryNestedJarPath = Join-Path (
    [System.IO.Path]::GetTempPath()
) ("bci-spring-kafka-test-$([guid]::NewGuid().ToString('N')).jar")

try {
    [System.IO.File]::WriteAllBytes(
        $temporaryNestedJarPath,
        (Read-ZipEntryBytes -ArchivePath $JarPath -EntryName $nestedJarEntryName)
    )
    $classBytes = Read-ZipEntryBytes -ArchivePath $temporaryNestedJarPath -EntryName $zookeeperClassEntryName
    $oldOffsetList = @(Find-SequenceOffsets -Content $classBytes -Sequence $oldTickSequence)
    $newOffsetList = @(Find-SequenceOffsets -Content $classBytes -Sequence $newTickSequence)

    if ($oldOffsetList.Count -eq 0 -and $newOffsetList.Count -eq 1) {
        Write-Output 'Embedded ZooKeeper tick is already 3000 ms.'
        return
    }
    if ($oldOffsetList.Count -ne 1 -or $newOffsetList.Count -ne 0) {
        throw (
            "Unexpected EmbeddedZookeeper bytecode: " +
            "old_sequence_count=$($oldOffsetList.Count) " +
            "new_sequence_count=$($newOffsetList.Count)"
        )
    }

    $tickOffset = $oldOffsetList[0]
    $classBytes[$tickOffset + 1] = 0x0B
    $classBytes[$tickOffset + 2] = 0xB8
    Replace-ZipEntry -ArchivePath $temporaryNestedJarPath -EntryName $zookeeperClassEntryName -Content $classBytes -CompressionLevel ([System.IO.Compression.CompressionLevel]::Optimal)
    Replace-ZipEntry -ArchivePath $JarPath -EntryName $nestedJarEntryName -Content ([System.IO.File]::ReadAllBytes($temporaryNestedJarPath)) -CompressionLevel ([System.IO.Compression.CompressionLevel]::NoCompression)

    Write-Output 'Patched embedded ZooKeeper tick from 800 ms to 3000 ms.'
}
finally {
    if (Test-Path -LiteralPath $temporaryNestedJarPath) {
        Remove-Item -LiteralPath $temporaryNestedJarPath -Force
    }
}
