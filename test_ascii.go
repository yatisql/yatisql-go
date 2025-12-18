package main

import (
	"fmt"
	"github.com/fatih/color"
)

func main() {
	bannerColor := color.New(color.FgYellow, color.Bold)
	
	// Test different Q designs
	fmt.Println("Testing Q designs:")
	fmt.Println()
	
	// Option 1: Q with tail
	bannerColor.Println("██╗   ██╗ █████╗ ████████╗██╗███████╗ ██████╗ ██████╗ ██╗     ")
	bannerColor.Println("╚██╗ ██╔╝██╔══██╗╚══██╔══╝██║██╔════╝██╔═══██╗██╔══██╗██║     ")
	bannerColor.Println(" ╚████╔╝ ███████║   ██║   ██║███████╗██║   ██║██████╔╝██║     ")
	bannerColor.Println("  ╚██╔╝  ██╔══██║   ██║   ██║╚════██║██║   ██║██╔══██╗██║     ")
	bannerColor.Println("   ██║   ██║  ██║   ██║   ██║███████║╚██████╔╝██║  ██║███████╗")
	bannerColor.Println("   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚══════╝")
	fmt.Println()
}

