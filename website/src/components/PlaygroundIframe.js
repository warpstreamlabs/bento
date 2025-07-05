import React, { useEffect, useRef } from "react";
import { useColorMode } from "@docusaurus/theme-common";

export default function PlaygroundIframe({ src, ...props }) {
  const iframeRef = useRef(null);
  const { colorMode } = useColorMode();

  useEffect(() => {
    const iframe = iframeRef.current;
    if (!iframe) return;

    const sendThemeToIframe = () => {
      if (iframe.contentWindow) {
        iframe.contentWindow.postMessage(
          {
            type: "docusaurus-theme-change",
            theme: colorMode,
          },
          "*"
        );
      }
    };

    const handleLoad = () => {
      // Wait for iframe to be ready
      setTimeout(sendThemeToIframe, 100);
    };

    // Send theme when colorMode changes
    sendThemeToIframe();

    // Listen for iframe load
    iframe.addEventListener("load", handleLoad);

    // Listen for theme requests from iframe
    const handleMessage = (event) => {
      if (event.data.type === "request-theme") {
        sendThemeToIframe();
      }
    };

    window.addEventListener("message", handleMessage);

    return () => {
      iframe.removeEventListener("load", handleLoad);
      window.removeEventListener("message", handleMessage);
    };
  }, [colorMode]);

  return <iframe ref={iframeRef} src={src} {...props} />;
}
