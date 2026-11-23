#!/usr/bin/env fsx
open System
open System.IO

#r "System.Configuration"
open System.Configuration

#load "../src/Fsdk/Misc.fs"
#load "../src/Fsdk/Process.fs"

open Fsdk
open Fsdk.Process

let sourceDir = DirectoryInfo __SOURCE_DIRECTORY__

let sample =
    Path.Combine(sourceDir.FullName, "testProcessConcurrencySample.fsx")
    |> FileInfo

let mutable retryCount = 0

let command =
#if !LEGACY_FRAMEWORK
    "dotnet"
#else
    if Misc.GuessPlatform() = Misc.Platform.Windows then
        // HACK: we should call fsx here but then we would get this problem in
        // the tests: error FS0193: The process cannot access the file 'D:\a\fsx\fsx\test\bin\FSharp.Core.dll' because it is being used by another process.
        match Process.VsWhere "**\\fsi.exe" with
        | None -> failwith "fsi.exe not found"
        | Some fsiExe -> fsiExe
    else
        // FIXME: extract PREFIX from build.config instead of assuming default
        "/usr/local/bin/fsx"
#endif

let validOutputs =
    [
        [| "foo"; "bar"; "baz" |]
        [| "bar"; "foo"; "baz" |]
        [| "foo"; "baz"; "bar" |]
    ]

while (retryCount < 20) do //this is a stress test
    let procResult =
        Process.Execute(
            {
                Command = command
#if !LEGACY_FRAMEWORK
                Arguments = sprintf "fsi %s" sample.FullName
#else
                Arguments = sample.FullName
#endif
            },
            Echo.Off
        )

    let lines =
        procResult
            .UnwrapDefault(false)
            .Split(
                [| Environment.NewLine; "\n"; "\r\n" |],
                StringSplitOptions.RemoveEmptyEntries
            )

    let isValid = validOutputs |> List.exists(fun expected -> lines = expected)

    if not isValid then
        Console.Error.WriteLine(
            sprintf
                "Stress test failed, got `%A`, expected complete non-interleaved lines"
                lines
        )

        Environment.Exit 1

    retryCount <- retryCount + 1

Console.WriteLine "Success"
Environment.Exit 0
