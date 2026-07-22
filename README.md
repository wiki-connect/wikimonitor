# WikiMonitor

WikiMonitor is a comprehensive tool designed to monitor Wikimedia's RecentChanges stream in real-time. It empowers users to define custom filter rules using Spring Expression Language (SpEL) to automatically flag specific edits, detect vandalism, or track specific patterns across Wikimedia projects.

## Features

-   **Real-time Monitoring**: Connects to the global Wikimedia `recentchange` stream via Server-Sent Events (SSE).
-   **Custom Abuse Filters**: Define complex filter rules using SpEL (Spring Expression Language) to analyze edits as they happen.
-   **Multi-User Support**: Individual users can maintain their own set of private filter rules.
-   **OAuth2 Integration**: Secure sign-in and authentication using Wikimedia OAuth2 (via Meta-Wiki or other MediaWiki instances).
-   **Live Updates**: Filtered events are broadcasted to the web interface in real-time.
-   **Responsive UI**: Modern web interface for managing filters and viewing live streams.

## Prerequisites

-   **Java**: Version 21 or higher.
-   **Diff Utilities**: `diff` command line tool (usually available on Linux/Unix).
-   **Wikimedia Account**: Required for OAuth2 authentication.
-   **Redis**: Used as a short-window replay cache so SSE clients can resume their event stream after the 30-minute connection rotation without losing flagged edits. On Toolforge the shared instance at `redis.svc.tools.eqiad1.wikimedia.cloud:6379` is used by default; locally you can run `redis-server` and override `REDIS_HOST`/`REDIS_PORT`.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/yourusername/wikiconnect.git
    cd wikiconnect/wikimonitor
    ```

2.  **Configure Environment Variables:**
    The application uses standard Spring Boot property resolution. A `.env` file is **not** loaded automatically. You can provide the required variables using any of the following methods:

    **Option A: System environment variables**
    ```bash
    export ACCESS_TOKEN=your_wikidata_access_token
    export MEDIAWIKI_CLIENT_ID=your_oauth_client_id
    export MEDIAWIKI_CLIENT_SECRET=your_oauth_client_secret
    export REQUIRE_ROLLBACK_RIGHT=true
    ```

    **Option B: `application.properties` or `application.yml`**
    Add the values to `src/main/resources/application.properties`:
    ```properties
    ACCESS_TOKEN=your_wikidata_access_token
    MEDIAWIKI_CLIENT_ID=your_oauth_client_id
    MEDIAWIKI_CLIENT_SECRET=your_oauth_client_secret
    REQUIRE_ROLLBACK_RIGHT=true
    ```

    **Option C: Command-line arguments**
    ```bash
    java -jar target/wikimonitor.jar \
      --ACCESS_TOKEN=your_wikidata_access_token \
      --MEDIAWIKI_CLIENT_ID=your_oauth_client_id \
      --MEDIAWIKI_CLIENT_SECRET=your_oauth_client_secret \
      --REQUIRE_ROLLBACK_RIGHT=true
    ```

    **Option D: IDE run configuration**
    Add the variables to your IDE's run/debug configuration as environment variables.

    | Variable | Description |
    |---|---|
    | `ACCESS_TOKEN` | Wikimedia owner-only OAuth2 access token for API interactions |
    | `MEDIAWIKI_CLIENT_ID` | OAuth2 client ID registered on Meta-Wiki |
    | `MEDIAWIKI_CLIENT_SECRET` | OAuth2 client secret |
    | `REQUIRE_ROLLBACK_RIGHT` | Set to `false` to bypass rollback rights check during local testing (default: `true`) |
    | `SSE_TIMEOUT_MS` | Per-client SSE connection timeout in milliseconds (default: `1800000`, i.e. 30 minutes). On timeout the browser auto-reconnects with `Last-Event-ID` and the server replays missed events. |
    | `SSE_EVENT_CACHE_MAX_SIZE` | Max number of recent events kept in the Redis replay cache (default: `1000`) |
    | `SSE_REDIS_KEY_PREFIX` | Prefix for Redis keys to namespace per deployment (default: `wikimonitor`) |
    | `REDIS_HOST` | Redis host (default: `redis.svc.tools.eqiad1.wikimedia.cloud` — Toolforge shared Redis) |
    | `REDIS_PORT` | Redis port (default: `6379`) |
    | `REDIS_PASSWORD` | Redis password if required (default: empty) |

    *Note: You need to register an OAuth2 consumer on Meta-Wiki or your target MediaWiki instance to obtain the Client ID and Secret. The `ACCESS_TOKEN` is used for initial API interactions or bot actions.*

3.  **Build the project:**
    ```bash
    mvn clean install
    ```

## Usage

1.  **Run the application:**
    ```bash
    mvn spring-boot:run
    ```
    Alternatively, run the built jar file:
    ```bash
    java -jar target/wikimonitor.jar
    ```

2.  **Access the Dashboard:**
    Open your web browser and navigate to `http://localhost:8000`.

3.  **Database Migrations (Flyway):**
    This project uses [Flyway](https://flywaydb.org/) for database schema versioning. Migrations run automatically when the application starts.
    -   **Create New Migrations:** Add `.sql` files to `src/main/resources/db/migration/` following the naming convention `V<Version>__<Description>.sql` (e.g., `V2__add_new_table.sql`).
    -   **Run Migrations Manually:**
        ```bash
        mvn flyway:migrate
        ```
    -   **Repair Failed Migrations:** If a migration fails (e.g., due to a syntax error), fix the SQL file and run:
        ```bash
        mvn flyway:repair
        ```

4.  **Login:**
    Click the login button to authenticate using your Wikimedia account via OAuth2.

5.  **Define Filters:**
    Navigate to the settings or filter management page to add SpEL-based rules.
     *Example Rule:*
    ```spel
    # Flag edits by specific user or containing specific text
    user == 'VandalUser' || title matches '.*Spam.*'
    ```

## Configuration

The application uses `src/main/resources/application.properties` for core settings:

-   **Server Port**: `server.port=8000`
-   **Database**: MySQL (managed by Flyway)
-   **JPA/Hibernate**: `spring.jpa.hibernate.ddl-auto=validate` ensuring schema changes are done through Flyway.

## Project Structure

-   `src/main/java`:
    -   `org.qrdlife.wikiconnect.wikimonitor`: Main application package.
    -   `controller`: Spring MVC and REST controllers for handling web requests.
    -   `service`: Core business logic including `WikiStreamService` (SSE), `AbuseFilterService` (SpEL filtering), and `OAuth2Service`.
    -   `model`: JPA Entities (`User`, `RecentChange`).
-   `src/main/resources`:
    -   `templates`: Thymeleaf templates for the frontend.
    -   `static`: Static assets (CSS, JS).
    -   `application.properties`: Configuration file.

## Technologies Used

-   **Backend**: Java 21, Spring Boot 4.1.0
-   **Database**: MySQL, Flyway (Migrations)
-   **Streaming**: OkHttp (SSE)
-   **Authentication**: ScribeJava (OAuth2)
-   **Templating**: Thymeleaf
-   **Utilities**: Java Diff Utils, Jsoup, Caffeine (Caching)

## License

This project is licensed under the GNU General Public License v3.0 (GPLv3). See the `LICENSE` file for details.
