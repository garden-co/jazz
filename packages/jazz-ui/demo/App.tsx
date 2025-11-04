import { useState } from "react";
import { ThemeProvider, useTheme } from "next-themes";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "../src/components/accordion";
import { Alert, AlertDescription, AlertTitle } from "../src/components/alert";
import { Avatar, AvatarFallback, AvatarImage } from "../src/components/avatar";
import { Badge } from "../src/components/badge";
import { Button } from "../src/components/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "../src/components/card";
import { Checkbox } from "../src/components/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "../src/components/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "../src/components/dropdown-menu";
import { Input } from "../src/components/input";
import { Label } from "../src/components/label";
import { NativeSelect } from "../src/components/native-select";
import { RadioGroup, RadioGroupItem } from "../src/components/radio-group";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../src/components/select";
import { Separator } from "../src/components/separator";
import { Spinner } from "../src/components/spinner";
import { Switch } from "../src/components/switch";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../src/components/table";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "../src/components/tabs";
import { Textarea } from "../src/components/textarea";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "../src/components/tooltip";
import { Toaster } from "../src/components/sonner";
import { toast } from "sonner";
import { Bell, User, Settings, Mail, Moon, Sun } from "lucide-react";

function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  return (
    <Button
      variant="outline"
      size="icon"
      onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
    >
      <Sun className="h-[1.2rem] w-[1.2rem] scale-100 rotate-0 transition-all dark:scale-0 dark:-rotate-90" />
      <Moon className="absolute h-[1.2rem] w-[1.2rem] scale-0 rotate-90 transition-all dark:scale-100 dark:rotate-0" />
      <span className="sr-only">Toggle theme</span>
    </Button>
  );
}

function DemoContent() {
  const [checked, setChecked] = useState(false);
  const [switchOn, setSwitchOn] = useState(false);

  return (
    <div className="bg-background min-h-screen">
      <Toaster />
      <div className="container mx-auto px-4 py-8">
        <header className="mb-12 flex items-center justify-between">
          <div>
            <h1 className="mb-2 text-4xl font-bold">Jazz UI Components</h1>
            <p className="text-muted-foreground text-lg">
              Interactive demo of all available components
            </p>
          </div>
        </header>
        <div className="sticky top-4 flex justify-end">
          <ThemeToggle />
        </div>

        <div className="flex flex-col gap-16">
          {/* Buttons */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Buttons</h2>
            <div className="flex flex-wrap gap-4">
              <Button>Default</Button>
              <Button variant="secondary">Secondary</Button>
              <Button variant="destructive">Destructive</Button>
              <Button variant="outline">Outline</Button>
              <Button variant="ghost">Ghost</Button>
              <Button variant="link">Link</Button>
              <Button size="sm">Small</Button>
              <Button size="lg">Large</Button>
              <Button disabled>Disabled</Button>
            </div>
          </section>

          {/* Inputs & Forms */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Inputs & Forms</h2>
            <div className="grid max-w-2xl gap-6">
              <div className="flex flex-col gap-2">
                <Label htmlFor="email">Email</Label>
                <Input id="email" type="email" placeholder="Email" />
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="password">Password</Label>
                <Input id="password" type="password" placeholder="Password" />
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="message">Message</Label>
                <Textarea id="message" placeholder="Type your message here." />
              </div>

              <div className="flex items-center gap-2">
                <Checkbox
                  id="terms"
                  checked={checked}
                  onCheckedChange={setChecked}
                />
                <Label htmlFor="terms">Accept terms and conditions</Label>
              </div>

              <div className="flex items-center gap-2">
                <Switch
                  id="airplane-mode"
                  checked={switchOn}
                  onCheckedChange={setSwitchOn}
                />
                <Label htmlFor="airplane-mode">Airplane Mode</Label>
              </div>

              <div className="flex flex-col gap-2">
                <Label>Radio Group</Label>
                <RadioGroup defaultValue="option-one">
                  <div className="flex items-center gap-2">
                    <RadioGroupItem value="option-one" id="option-one" />
                    <Label htmlFor="option-one">Option One</Label>
                  </div>
                  <div className="flex items-center gap-2">
                    <RadioGroupItem value="option-two" id="option-two" />
                    <Label htmlFor="option-two">Option Two</Label>
                  </div>
                  <div className="flex items-center gap-2">
                    <RadioGroupItem value="option-three" id="option-three" />
                    <Label htmlFor="option-three">Option Three</Label>
                  </div>
                </RadioGroup>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="native-select">Native Select</Label>
                <NativeSelect id="native-select">
                  <option value="1">Option 1</option>
                  <option value="2">Option 2</option>
                  <option value="3">Option 3</option>
                </NativeSelect>
              </div>

              <div className="flex flex-col gap-2">
                <Label htmlFor="select">Select (Radix)</Label>
                <Select>
                  <SelectTrigger className="w-[200px]">
                    <SelectValue placeholder="Select a fruit" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="apple">Apple</SelectItem>
                    <SelectItem value="banana">Banana</SelectItem>
                    <SelectItem value="orange">Orange</SelectItem>
                    <SelectItem value="grape">Grape</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </section>

          {/* Cards */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Cards</h2>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              <Card>
                <CardHeader>
                  <CardTitle>Card Title</CardTitle>
                  <CardDescription>Card Description</CardDescription>
                </CardHeader>
                <CardContent>
                  <p>Card Content goes here.</p>
                </CardContent>
                <CardFooter>
                  <Button>Action</Button>
                </CardFooter>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Notifications</CardTitle>
                  <CardDescription>You have 3 unread messages.</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-col gap-2">
                    <p className="text-sm">Your profile has been updated.</p>
                    <p className="text-muted-foreground text-sm">2 hours ago</p>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Statistics</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold">$45,231.89</div>
                  <p className="text-muted-foreground text-xs">
                    +20.1% from last month
                  </p>
                </CardContent>
              </Card>
            </div>
          </section>

          {/* Alerts */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Alerts</h2>
            <div className="max-w-2xl flex flex-col gap-4">
              <Alert variant="info">
                <AlertTitle>Default Alert</AlertTitle>
                <AlertDescription>
                  This is a default alert with some information.
                </AlertDescription>
              </Alert>

              <Alert variant="destructive">
                <AlertTitle>Error Alert</AlertTitle>
                <AlertDescription>
                  Something went wrong. Please try again.
                </AlertDescription>
              </Alert>
              <Alert variant="warning">
                <AlertTitle>Warning Alert</AlertTitle>
                <AlertDescription>
                  This is a warning alert with some information.
                </AlertDescription>
              </Alert>
            </div>
          </section>

          {/* Badges */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Badges</h2>
            <div className="flex flex-wrap gap-4">
              <Badge>Default</Badge>
              <Badge variant="secondary">Secondary</Badge>
              <Badge variant="destructive">Destructive</Badge>
              <Badge variant="outline">Outline</Badge>
            </div>
          </section>

          {/* Avatar */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Avatar</h2>
            <div className="flex gap-4">
              <Avatar>
                <AvatarImage
                  src="https://github.com/shadcn.png"
                  alt="@shadcn"
                />
                <AvatarFallback>CN</AvatarFallback>
              </Avatar>
              <Avatar>
                <AvatarFallback>AB</AvatarFallback>
              </Avatar>
              <Avatar>
                <AvatarFallback>
                  <User className="h-4 w-4" />
                </AvatarFallback>
              </Avatar>
            </div>
          </section>

          {/* Separator */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Separator</h2>
            <div className="max-w-md">
              <div className="flex flex-col gap-1">
                <h4 className="text-sm font-medium">Jazz UI Components</h4>
                <p className="text-muted-foreground text-sm">
                  A collection of reusable components.
                </p>
              </div>
              <Separator className="my-4" />
              <div className="flex h-5 items-center gap-4 text-sm">
                <div>Home</div>
                <Separator orientation="vertical" />
                <div>About</div>
                <Separator orientation="vertical" />
                <div>Contact</div>
              </div>
            </div>
          </section>

          {/* Tabs */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Tabs</h2>
            <Tabs defaultValue="account" className="max-w-md">
              <TabsList>
                <TabsTrigger value="account">Account</TabsTrigger>
                <TabsTrigger value="password">Password</TabsTrigger>
                <TabsTrigger value="settings">Settings</TabsTrigger>
              </TabsList>
              <TabsContent value="account" className="flex flex-col gap-2">
                <p className="text-muted-foreground text-sm">
                  Make changes to your account here.
                </p>
                <Input placeholder="Name" />
                <Input placeholder="Email" />
              </TabsContent>
              <TabsContent value="password" className="flex flex-col gap-2">
                <p className="text-muted-foreground text-sm">
                  Change your password here.
                </p>
                <Input type="password" placeholder="Current password" />
                <Input type="password" placeholder="New password" />
              </TabsContent>
              <TabsContent value="settings" className="flex flex-col gap-2">
                <p className="text-muted-foreground text-sm">
                  Manage your settings here.
                </p>
                <div className="flex items-center gap-2">
                  <Switch id="notifications" />
                  <Label htmlFor="notifications">Enable notifications</Label>
                </div>
              </TabsContent>
            </Tabs>
          </section>

          {/* Accordion */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Accordion</h2>
            <Accordion type="single" collapsible className="max-w-md">
              <AccordionItem value="item-1">
                <AccordionTrigger>Is it accessible?</AccordionTrigger>
                <AccordionContent>
                  Yes. It adheres to the WAI-ARIA design pattern.
                </AccordionContent>
              </AccordionItem>
              <AccordionItem value="item-2">
                <AccordionTrigger>Is it styled?</AccordionTrigger>
                <AccordionContent>
                  Yes. It comes with default styles that matches the other
                  components aesthetic.
                </AccordionContent>
              </AccordionItem>
              <AccordionItem value="item-3">
                <AccordionTrigger>Is it animated?</AccordionTrigger>
                <AccordionContent>
                  Yes. It's animated by default, but you can disable it if you
                  prefer.
                </AccordionContent>
              </AccordionItem>
            </Accordion>
          </section>

          {/* Table */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Table</h2>
            <div className="max-w-2xl">
              <Table>
                <TableCaption>A list of your recent invoices.</TableCaption>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[100px]">Invoice</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Method</TableHead>
                    <TableHead className="text-right">Amount</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  <TableRow>
                    <TableCell className="font-medium">INV001</TableCell>
                    <TableCell>Paid</TableCell>
                    <TableCell>Credit Card</TableCell>
                    <TableCell className="text-right">$250.00</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="font-medium">INV002</TableCell>
                    <TableCell>Pending</TableCell>
                    <TableCell>PayPal</TableCell>
                    <TableCell className="text-right">$150.00</TableCell>
                  </TableRow>
                  <TableRow>
                    <TableCell className="font-medium">INV003</TableCell>
                    <TableCell>Unpaid</TableCell>
                    <TableCell>Bank Transfer</TableCell>
                    <TableCell className="text-right">$350.00</TableCell>
                  </TableRow>
                </TableBody>
              </Table>
            </div>
          </section>

          {/* Dialog */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Dialog</h2>
            <Dialog>
              <DialogTrigger asChild>
                <Button>Open Dialog</Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Are you absolutely sure?</DialogTitle>
                  <DialogDescription>
                    This action cannot be undone. This will permanently delete
                    your account and remove your data from our servers.
                  </DialogDescription>
                </DialogHeader>
                <DialogFooter>
                  <Button variant="outline">Cancel</Button>
                  <Button>Confirm</Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </section>

          {/* Dropdown Menu */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Dropdown Menu</h2>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline">
                  <Settings className="mr-2 h-4 w-4" />
                  Open Menu
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent>
                <DropdownMenuLabel>My Account</DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem>
                  <User className="mr-2 h-4 w-4" />
                  Profile
                </DropdownMenuItem>
                <DropdownMenuItem>
                  <Settings className="mr-2 h-4 w-4" />
                  Settings
                </DropdownMenuItem>
                <DropdownMenuItem>
                  <Mail className="mr-2 h-4 w-4" />
                  Messages
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </section>

          {/* Tooltip */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Tooltip</h2>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button variant="outline">Hover me</Button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>Add to library</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </section>

          {/* Toast (Sonner) */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Toast (Sonner)</h2>
            <div className="flex flex-wrap gap-4">
              <Button onClick={() => toast("This is a toast message")}>
                Show Toast
              </Button>
              <Button onClick={() => toast.success("Successfully saved!")}>
                Success Toast
              </Button>
              <Button onClick={() => toast.error("Something went wrong!")}>
                Error Toast
              </Button>
              <Button onClick={() => toast.info("This is some information")}>
                Info Toast
              </Button>
              <Button onClick={() => toast.warning("This is a warning")}>
                Warning Toast
              </Button>
            </div>
          </section>

          {/* Spinner */}
          <section>
            <h2 className="mb-4 text-2xl font-semibold">Spinner</h2>
            <div className="flex items-center gap-4">
              <Spinner size="sm" />
              <Spinner />
              <Spinner size="lg" />
            </div>
          </section>
        </div>

        <footer className="text-muted-foreground mt-16 border-t pt-8 text-center">
          <p>Jazz UI Components Demo • Built with React + Vite</p>
        </footer>
      </div>
    </div>
  );
}

function App() {
  return (
    <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
      <DemoContent />
    </ThemeProvider>
  );
}

export default App;
