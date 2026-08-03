export default {
    async fetch(request, env) {
        const url = new URL(request.url);
        const target = url.searchParams.get('url');

        let targetUrl;
        if (target) {
            targetUrl = decodeURIComponent(target);
        } else {
            const ref = request.headers.get('Referer') || request.headers.get('Origin') || '';
            if (!ref) {
                return new Response('No URL provided', { status: 400 });
            }
            targetUrl = ref;
        }

        const resp = await fetch(targetUrl, {
            headers: {
                'User-Agent': 'v2rayN/5.38'
            }
        });

        let body = await resp.text();
        body = body.trim();

        try {
            const decoded = atob(body);
            if (decoded.indexOf('://') > -1) {
                body = decoded;
            }
        } catch (e) {}

        const lines = body.split('\n').filter(function(l) {
            const t = l.trim();
            return t && (t.startsWith('vless://') || t.startsWith('trojan://') || t.startsWith('vmess://') || t.startsWith('ss://'));
        });

        return new Response(lines.join('\n'), {
            status: 200,
            headers: {
                'Content-Type': 'text/plain; charset=utf-8',
                'Access-Control-Allow-Origin': '*'
            }
        });
    }
};
